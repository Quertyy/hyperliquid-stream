//! `hl-stream stream`: JSON lines on stdout.

use crate::{
    Format, StreamArgs,
    records::{self, ActionRecord, SummaryRecord, accepts_action, accepts_transaction},
    shutdown_signal,
};
use anyhow::Result;
use hyperliquid_stream::{Client, Event};
use hyperliquid_ws::KindFilter;
use serde::Serialize;
use std::io::{self, BufWriter, Write};

pub(crate) async fn stream(client: &mut Client, args: &StreamArgs) -> Result<()> {
    let mut writer = Writer {
        out: BufWriter::with_capacity(1 << 16, io::stdout().lock()),
        args,
        filter: KindFilter::new(args.kinds.iter().cloned()),
    };
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        let event = tokio::select! {
            event = client.next_event() => event,
            () = &mut shutdown => break,
        };
        let Some(event) = event else {
            break;
        };
        // Write everything already received, then flush once.
        let write_ready = || {
            let mut next = Some(event);
            while let Some(event) = next {
                writer.write(&event)?;
                next = client.try_next_event();
            }
            writer.out.flush()
        };
        match write_ready() {
            // The reader went away, as with `hl-stream | head`.
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => break,
            result => result?,
        }
    }
    Ok(())
}

struct Writer<'a, W> {
    out: W,
    args: &'a StreamArgs,
    filter: KindFilter,
}

impl<W: Write> Writer<'_, W> {
    fn write(&mut self, event: &Event) -> io::Result<()> {
        match event {
            Event::Transaction(tx) => match self.args.format {
                Format::Actions => {
                    for (index, action) in tx.actions().iter().enumerate() {
                        if accepts_action(&self.filter, action) {
                            self.line(&ActionRecord::new(tx, index, action))?;
                        }
                    }
                    Ok(())
                }
                Format::Transactions if accepts_transaction(&self.filter, tx) => {
                    self.line(&records::transaction(tx))
                }
                Format::Summary if accepts_transaction(&self.filter, tx) => {
                    self.line(&SummaryRecord::new(tx))
                }
                Format::Transactions | Format::Summary => Ok(()),
            },
            Event::Block(block) if self.args.blocks => self.line(&records::block(block)),
            Event::Stats(stats) if self.args.stats => self.line(&records::stats(stats)),
            _ => Ok(()),
        }
    }

    fn line(&mut self, record: &impl Serialize) -> io::Result<()> {
        serde_json::to_writer(&mut self.out, record)?;
        self.out.write_all(b"\n")
    }
}
