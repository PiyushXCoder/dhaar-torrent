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
    Element, Length, Subscription, Task,
    widget::{button, column, container, progress_bar, row, scrollable, text},
};
use tokio::runtime::Runtime;

/// How often the window redraws. The library samples its own status once a
/// second, so asking more often only repeats values.
const REFRESH: Duration = Duration::from_secs(1);

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
            client.add(&path);
        }
        client
    }

    fn add(&mut self, path: &Path) {
        let download = match Download::from_torrent_file(path) {
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
            Message::TorrentPicked(Some(path)) => self.add(&path),
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

    fn view(&self) -> Element<'_, Message> {
        let mut rows: Vec<Element<'_, Message>> = vec![
            row(vec![
                text("Downloads").size(22).into(),
                button("Add torrent").on_press(Message::AddTorrent).into(),
            ])
            .spacing(16)
            .align_y(iced::Alignment::Center)
            .into(),
        ];

        if let Some(error) = self.error.as_ref() {
            rows.push(text(error.clone()).size(13).into());
        }

        if self.downloads.is_empty() {
            rows.push(
                text("Nothing downloading yet — add a .torrent file to begin.")
                    .size(14)
                    .into(),
            );
        }

        rows.extend(
            self.downloads
                .iter()
                .enumerate()
                .map(|(index, entry)| entry.view(index)),
        );

        container(scrollable(column(rows).spacing(20)).height(Length::Fill))
            .padding(24)
            .into()
    }
}

impl Entry {
    fn view(&self, index: usize) -> Element<'_, Message> {
        let status = self.handle.status();

        let heading = row(vec![
            text(self.name.clone()).size(16).into(),
            text(describe(&status)).size(13).into(),
            button("Remove").on_press(Message::Remove(index)).into(),
        ])
        .spacing(12)
        .align_y(iced::Alignment::Center);

        column(vec![
            heading.into(),
            progress_bar(0.0..=1.0, status.progress() as f32)
                .girth(10)
                .into(),
            text(format!(
                "{}/{} pieces · {} peers · {} in flight · down {} · up {}",
                status.pieces.completed_pieces,
                status.pieces.total_pieces,
                status.active_peers,
                status.in_flight_pieces,
                rate(status.download_rate),
                rate(status.upload_rate),
            ))
            .size(12)
            .into(),
            text(format!(
                "{} of {} · wasted {} · failed hashes {}",
                bytes(status.pieces.verified_bytes),
                bytes(status.pieces.total_bytes),
                bytes(status.wasted_bytes),
                status.hash_failures,
            ))
            .size(12)
            .into(),
        ])
        .spacing(6)
        .into()
    }
}

fn describe(status: &DownloadStatus) -> String {
    let state = match status.state {
        DownloadState::Starting => "starting",
        DownloadState::Downloading => "downloading",
        DownloadState::Seeding => "seeding",
    };
    format!("{state} · {:.1}%", status.progress() * 100.0)
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
        .window_size((640.0, 460.0))
        .run()
}
