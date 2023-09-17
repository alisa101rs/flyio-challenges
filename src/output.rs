use std::io::{stdout, Write};

use serde::Serialize;

#[derive(Copy, Clone)]
pub struct Output {}

impl Output {
    pub fn lock() -> Self {
        Self {}
    }

    pub fn write<R>(&self, iter: impl IntoIterator<Item = R>)
    where
        R: Serialize + std::fmt::Debug,
    {
        let mut stdout = stdout().lock();
        for message in iter {
            serde_json::ser::to_writer(&mut stdout, &message).expect("failed to write to stdout");
            stdout.write(b"\n").expect("failed to write new line");
        }
    }
}
