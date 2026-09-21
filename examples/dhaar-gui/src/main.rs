//! The reference client for the `dhaar-torrent` library.
//!
//! Everything interesting lives in the library; this is what using it looks
//! like from the outside. It is kept deliberately small, so if something here
//! starts to feel clever it probably belongs in the library instead.

use std::{env, path::Path, path::PathBuf, time::Duration};

use dhaar_torrent::{
    Download, DownloadHandle,
    status::{DownloadState, DownloadStatus},
};
use iced::{
    Background, Border, Color, Element, Font, Gradient, Length, Subscription, Task, Theme,
    font::{Family, Weight},
    gradient,
    widget::{button, column, container, progress_bar, row, scrollable, text},
};
use tokio::runtime::Runtime;

/// JetBrains Mono Nerd Font, bundled rather than looked up by name: the icon
/// glyphs live in the Unicode private use area, so a machine without the font
/// renders them as tofu rather than falling back to anything sensible.
const FONT_REGULAR: &[u8] = include_bytes!("../fonts/JetBrainsMonoNerdFontPropo-Regular.ttf");
const FONT_BOLD: &[u8] = include_bytes!("../fonts/JetBrainsMonoNerdFontPropo-Bold.ttf");

/// The family name is read out of the font file itself, so it has to match
/// what the file declares -- `fc-query -f '%{family}'` prints it.
const FONT_FAMILY: &str = "JetBrainsMono Nerd Font Propo";

const UI_FONT: Font = Font {
    family: Family::Name(FONT_FAMILY),
    ..Font::DEFAULT
};

const UI_FONT_BOLD: Font = Font {
    family: Family::Name(FONT_FAMILY),
    weight: Weight::Bold,
    ..Font::DEFAULT
};

/// Dracula's green, and the cyan it gets graded into across the progress bar.
const GREEN: Color = Color::from_rgb(0.314, 0.980, 0.482);
const CYAN: Color = Color::from_rgb(0.545, 0.914, 0.992);
const PINK: Color = Color::from_rgb(1.0, 0.475, 0.776);
const MUTED: Color = Color::from_rgb(0.68, 0.70, 0.80);

/// How often the window redraws. The library samples its own status once a
/// second, so asking more often only repeats values.
const REFRESH: Duration = Duration::from_secs(1);

/// The port the peer manager binds for incoming connections. Hardcoded, which
/// means only one download at a time can actually listen: the second bind of
/// the same port fails, and it fails inside a spawned task, so that download
/// simply never accepts a peer rather than reporting anything. Fine while this
/// is a one-torrent-at-a-time client; the fix is a single listener owned by
/// `Client` and shared across downloads.
const LISTENING_PORT: u16 = 6881;

#[derive(Debug, Clone)]
enum Message {
    AddTorrent,
    TorrentPicked(Option<PathBuf>),
    Remove(usize),
    Tick,
}

struct Client {
    /// One runtime for every download. The actors are spawned onto it rather
    /// than each download getting a thread, and the interface never runs on
    /// it — the two only meet through `DownloadHandle`, which reads its
    /// status without needing a runtime.
    runtime: Runtime,
    downloads: Vec<Entry>,
    error: Option<String>,
}

struct Entry {
    name: String,
    /// Identifies the torrent rather than the file it came from, so the same
    /// content added twice from two different `.torrent` files is caught.
    info_hash: [u8; 20],
    /// Dropping this stops the download, which is what removing a row does.
    handle: DownloadHandle,
}

impl Client {
    fn new() -> Self {
        let mut client = Self {
            runtime: Runtime::new().expect("a tokio runtime"),
            downloads: Vec::new(),
            error: None,
        };
        // Paths on the command line start immediately; the picker is the
        // normal route, this is the convenient one while working on it.
        for path in env::args().skip(1).map(PathBuf::from) {
            client.add(&path, LISTENING_PORT);
        }
        client
    }

    fn add(&mut self, path: &Path, listening_port: u16) {
        let download = match Download::from_torrent_file(path, listening_port) {
            Ok(download) => download,
            Err(e) => {
                self.error = Some(format!("Could not open {}: {e}", path.display()));
                return;
            }
        };

        let info_hash = download.torrent().info_hash;
        if self
            .downloads
            .iter()
            .any(|entry| entry.info_hash == info_hash)
        {
            self.error = Some(format!(
                "{} is already downloading",
                download.torrent().info.name
            ));
            return;
        }

        let name = download.torrent().info.name.clone();
        // `spawn` puts the actors on whichever runtime is current, so ours has
        // to be entered first. It returns immediately; nothing here blocks.
        let handle = {
            let _guard = self.runtime.enter();
            download.spawn()
        };

        self.downloads.push(Entry {
            name,
            info_hash,
            handle,
        });
        self.error = None;
    }

    fn remove(&mut self, index: usize) {
        if index >= self.downloads.len() {
            return;
        }
        // Dropping the handle aborts the download's tasks, so it is done
        // inside the runtime they belong to.
        let _guard = self.runtime.enter();
        self.downloads.remove(index);
    }

    fn title(&self) -> String {
        match self.downloads.len() {
            0 => "dhaar".to_owned(),
            1 => format!("{} — dhaar", self.downloads[0].name),
            count => format!("{count} downloads — dhaar"),
        }
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::AddTorrent => return Task::perform(pick_torrent(), Message::TorrentPicked),
            Message::TorrentPicked(Some(path)) => self.add(&path, LISTENING_PORT),
            Message::TorrentPicked(None) => {}
            Message::Remove(index) => self.remove(index),
            // Nothing to store: the status is read straight from the handles
            // when the view is built. The message exists to prompt a redraw.
            Message::Tick => {}
        }
        Task::none()
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(REFRESH).map(|_| Message::Tick)
    }

    fn theme(&self) -> Theme {
        Theme::Dracula
    }

    fn view(&self) -> Element<'_, Message> {
        let header = row(vec![
            text("󰇚 dhaar")
                .size(46)
                .font(UI_FONT_BOLD)
                .color(GREEN)
                .into(),
            text(format!("{} active", self.downloads.len()))
                .size(16)
                .color(MUTED)
                .width(Length::Fill)
                .into(),
            button(text("󰐕 add torrent").size(18).font(UI_FONT_BOLD))
                .padding([12, 20])
                .style(square(button::primary))
                .on_press(Message::AddTorrent)
                .into(),
        ])
        .spacing(18)
        .align_y(iced::Alignment::Center);

        let mut rows: Vec<Element<'_, Message>> = vec![header.into()];

        if let Some(error) = self.error.as_ref() {
            rows.push(
                container(text(format!("󰀦 {error}")).size(15).color(PINK))
                    .padding(14)
                    .width(Length::Fill)
                    .style(|_| container::Style {
                        background: Some(Background::Color(Color { a: 0.14, ..PINK })),
                        border: Border {
                            color: PINK,
                            width: 1.0,
                            radius: 0.0.into(),
                        },
                        ..container::Style::default()
                    })
                    .into(),
            );
        }

        if self.downloads.is_empty() {
            rows.push(
                container(
                    column(vec![
                        text("󰇧").size(72).color(MUTED).into(),
                        text("nothing downloading yet")
                            .size(22)
                            .font(UI_FONT_BOLD)
                            .color(MUTED)
                            .into(),
                        text("hit 󰐕 add torrent and pick a .torrent file")
                            .size(15)
                            .color(MUTED)
                            .into(),
                    ])
                    .spacing(10)
                    .align_x(iced::Alignment::Center),
                )
                .padding(50)
                .width(Length::Fill)
                .align_x(iced::Alignment::Center)
                .into(),
            );
        }

        rows.extend(
            self.downloads
                .iter()
                .enumerate()
                .map(|(index, entry)| entry.view(index)),
        );

        container(scrollable(column(rows).spacing(22)).height(Length::Fill))
            .padding(30)
            .into()
    }
}

/// The filled part of the bar, graded green to cyan so it reads as motion in a
/// still screenshot. `progress_bar::success` would give a flat green if this
/// ever needs to be calmer.
fn bar_style(_theme: &Theme) -> progress_bar::Style {
    progress_bar::Style {
        // Transparent: the track is the container behind this, so that the
        // rounded corners clip the fill rather than being painted over.
        background: Background::Color(Color::TRANSPARENT),
        bar: Background::Gradient(Gradient::Linear(
            gradient::Linear::new(std::f32::consts::FRAC_PI_2)
                .add_stop(0.0, GREEN)
                .add_stop(1.0, CYAN),
        )),
        border: Border::default(),
    }
}

/// The track behind the bar. Drawn as its own container because a translucent
/// `background` on the bar itself would sit under the fill as well.
fn track_style(_theme: &Theme) -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgba(1.0, 1.0, 1.0, 0.07))),
        ..container::Style::default()
    }
}

/// Wraps one of iced's built-in button styles and flattens its corners, so the
/// buttons match the square cards rather than the theme's rounded default.
fn square(
    base: impl Fn(&Theme, button::Status) -> button::Style,
) -> impl Fn(&Theme, button::Status) -> button::Style {
    move |theme, status| button::Style {
        border: Border {
            radius: 0.0.into(),
            ..base(theme, status).border
        },
        ..base(theme, status)
    }
}

fn card_style(theme: &Theme) -> container::Style {
    let palette = theme.extended_palette();
    container::Style {
        background: Some(Background::Color(palette.background.weak.color)),
        border: Border {
            color: Color::from_rgba(1.0, 1.0, 1.0, 0.10),
            width: 1.0,
            radius: 0.0.into(),
        },
        ..container::Style::default()
    }
}

impl Entry {
    fn view(&self, index: usize) -> Element<'_, Message> {
        let status = self.handle.status();

        let heading = row(vec![
            text(self.name.clone())
                .size(21)
                .font(UI_FONT_BOLD)
                .width(Length::Fill)
                .into(),
            text(describe(&status))
                .size(15)
                .color(state_color(&status))
                .into(),
            button(text("󰅖").size(17))
                .padding([8, 14])
                .style(square(button::danger))
                .on_press(Message::Remove(index))
                .into(),
        ])
        .spacing(14)
        .align_y(iced::Alignment::Center);

        // The number people screenshot, so it gets to be the only large thing
        // in the card besides the title.
        let hero = row(vec![
            text(rate(status.download_rate))
                .size(40)
                .font(UI_FONT_BOLD)
                .color(GREEN)
                .into(),
            text(format!("󰕒 {}", rate(status.upload_rate)))
                .size(17)
                .color(CYAN)
                .width(Length::Fill)
                .into(),
            text(format!("{:.1}%", status.progress() * 100.0))
                .size(30)
                .font(UI_FONT_BOLD)
                .into(),
        ])
        .spacing(16)
        .align_y(iced::Alignment::Center);

        let bar = container(
            progress_bar(0.0..=1.0, status.progress() as f32)
                .girth(26)
                .style(bar_style),
        )
        .style(track_style);

        container(
            column(vec![
                heading.into(),
                hero.into(),
                bar.into(),
                text(format!(
                    "󰄷 {}/{} pieces   󰀉 {} peers   󰑮 {} in flight",
                    status.pieces.completed_pieces,
                    status.pieces.total_pieces,
                    status.active_peers,
                    status.in_flight_pieces,
                ))
                .size(15)
                .color(MUTED)
                .into(),
                text(format!(
                    "󰋊 {} of {}   󰩹 {} wasted   󰀪 {} failed hashes",
                    bytes(status.pieces.verified_bytes),
                    bytes(status.pieces.total_bytes),
                    bytes(status.wasted_bytes),
                    status.hash_failures,
                ))
                .size(15)
                .color(MUTED)
                .into(),
            ])
            .spacing(12),
        )
        .padding(22)
        .style(card_style)
        .into()
    }
}

fn describe(status: &DownloadStatus) -> String {
    let state = match status.state {
        DownloadState::Starting => "⏳ starting",
        DownloadState::Downloading => "🔥 downloading",
        DownloadState::Finalizing => "🧩 finalizing",
        DownloadState::Seeding => "🌱 seeding",
    };
    state.to_owned()
}

fn state_color(status: &DownloadStatus) -> Color {
    match status.state {
        DownloadState::Starting | DownloadState::Finalizing => MUTED,
        DownloadState::Downloading => CYAN,
        DownloadState::Seeding => GREEN,
    }
}

async fn pick_torrent() -> Option<PathBuf> {
    rfd::AsyncFileDialog::new()
        .set_title("Choose a torrent")
        .add_filter("Torrent files", &["torrent"])
        .pick_file()
        .await
        .map(|file| file.path().to_path_buf())
}

fn bytes(count: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = count as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn rate(bytes_per_second: u64) -> String {
    format!("{}/s", bytes(bytes_per_second))
}

fn main() -> iced::Result {
    iced::application(Client::new, Client::update, Client::view)
        .title(Client::title)
        .subscription(Client::subscription)
        // `font` registers the bytes; `default_font` picks what to draw with.
        // Both are needed -- registering alone changes nothing.
        .font(FONT_REGULAR)
        .font(FONT_BOLD)
        .default_font(UI_FONT)
        .theme(Client::theme)
        .antialiasing(true)
        .window_size((980.0, 720.0))
        .run()
}
