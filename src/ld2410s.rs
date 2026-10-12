use crate::protocol;
use flume::{Receiver, Sender};
use futures::future::{Either, pending};
use log::warn;
use ringbuffer::{ConstGenericRingBuffer, RingBuffer};
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::sleep_until;

const MESSAGE_TIMEOUT: Duration = Duration::from_secs(13);
const MESSAGE_TIMEOUT_1: Duration = Duration::from_secs(31);
const RX_BUFFER_SIZE: usize = 128;
const CMD_QUEUE_SIZE: usize = 13;
const CMD_BUFFER_SIZE: usize = 0x80;

#[derive(Clone, Debug, PartialEq)]
pub enum State {
    Empty,
    Detected(u16),
    AutoThreshold(u8),
    Unknown,
}

enum PState {
    ProcessMessage,
    ContinueCommand,
    WaitEvent,
}

#[derive(PartialEq)]
enum ConfigEnabled {
    Yes,
    No,
    Asking,
}

pub struct Ld2410s<T> {
    state: PState,
    decoder: protocol::FrameDecoder,
    serial_stream: T,
    last_command_timestamp: Option<Instant>,
    current_command: Option<protocol::CommandRequest>,
    pending_command: Option<(protocol::CommandRequest, Instant)>,
    rx_buffer: ConstGenericRingBuffer<u8, RX_BUFFER_SIZE>,
    raw_rx_buffer: [u8; RX_BUFFER_SIZE],
    command_queue: Receiver<protocol::CommandRequest>,
    command_sender: Sender<protocol::CommandRequest>,
    config_enable: ConfigEnabled,
    command_buffer: Vec<u8>,
}

impl<T> Ld2410s<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(serial_stream: T) -> Self {
        let (command_sender, command_queue) = flume::bounded(CMD_QUEUE_SIZE);
        Self {
            state: PState::WaitEvent,
            decoder: protocol::FrameDecoder::new(),
            serial_stream,
            last_command_timestamp: None,
            current_command: None,
            pending_command: None,
            rx_buffer: ConstGenericRingBuffer::new(),
            raw_rx_buffer: [0; RX_BUFFER_SIZE],
            command_queue,
            command_sender,
            config_enable: ConfigEnabled::No,
            command_buffer: Vec::with_capacity(CMD_BUFFER_SIZE),
        }
    }

    pub async fn poll(&mut self) -> Result<State, Error> {
        loop {
            match self.state {
                PState::ProcessMessage => {
                    if let Some(r) = self.process_message() {
                        return r;
                    }
                }
                PState::ContinueCommand => {
                    self.continue_command().await?;
                }
                PState::WaitEvent => {
                    let command_expire =
                        self.last_command_timestamp
                            .map_or(Either::Left(pending()), |t| {
                                Either::Right(sleep_until(
                                    tokio::time::Instant::from_std(t) + MESSAGE_TIMEOUT,
                                ))
                            });
                    tokio::pin!(command_expire);

                    let command_expire_1 =
                        self.pending_command
                            .as_ref()
                            .map_or(Either::Left(pending()), |t| {
                                Either::Right(sleep_until(
                                    tokio::time::Instant::from_std(t.1) + MESSAGE_TIMEOUT_1,
                                ))
                            });
                    tokio::pin!(command_expire_1);

                    let next_command = if self.config_enable == ConfigEnabled::No {
                        Either::Left(self.command_queue.recv_async())
                    } else {
                        Either::Right(pending())
                    };

                    tokio::select! {
                        u = self.serial_stream.read(&mut self.raw_rx_buffer) => {
                            for i in 0..(u?) {
                                self.rx_buffer.enqueue(self.raw_rx_buffer[i]);
                            }
                            self.state = PState::ProcessMessage;
                        }
                        u = next_command => {
                            match u {
                                Ok(z) => { self.prepare_command(z).await?; }
                                Err(_) => { panic!("command channel dropped!"); }
                            }
                        }
                        _ = &mut command_expire => {
                            warn!("command timeout");
                            if self.config_enable == ConfigEnabled::Asking {
                                self.config_begin().await?;
                            }
                            else {
                                self.state = PState::ContinueCommand;
                            }
                        }
                        _ = &mut command_expire_1 => {
                            warn!("command timeout 1");
                            self.pending_command = self.command_queue.try_recv()
                                .map(|t| (t, Instant::now()))
                                .ok();
                            self.state = PState::ContinueCommand;
                        }
                    }
                }
            }
        }
    }

    fn process_message(&mut self) -> Option<Result<State, Error>> {
        while let Some(u) = self.rx_buffer.dequeue() {
            match self.decoder.feed(u) {
                Some(Ok(z)) => match z {
                    protocol::Response::DataNormal(d) => match d.target_status {
                        0 | 1 => {
                            return Some(Ok(State::Empty));
                        }
                        2 | 3 => {
                            return Some(Ok(State::Detected(d.distance.into())));
                        }
                        _ => {
                            return Some(Err(Error::new(ErrorKind::InvalidData, "invalide data")));
                        }
                    },
                    protocol::Response::DataMinimal(d) => match d.target_status {
                        0 | 1 => {
                            return Some(Ok(State::Empty));
                        }
                        2 | 3 => {
                            return Some(Ok(State::Detected(d.distance.into())));
                        }
                        _ => {
                            return Some(Err(Error::new(ErrorKind::InvalidData, "invalide data")));
                        }
                    },
                    protocol::Response::DataAutoThreshold(d) => {
                        match self.current_command {
                            Some(protocol::CommandRequest::RequestAutoThresholds(_)) => {
                                self.state = PState::ContinueCommand;
                            }
                            _ => {}
                        }
                        return Some(Ok(State::AutoThreshold(d.progress.to_bytes()[0])));
                    }
                    a => {
                        self.process_command_response(a);
                        self.state = PState::ContinueCommand;
                        return None;
                    }
                },
                Some(Err(e)) => {
                    return Some(Err(e));
                }
                None => {}
            }
        }
        self.state = PState::WaitEvent;
        None
    }

    async fn continue_command(&mut self) -> io::Result<()> {
        self.state = PState::ProcessMessage;

        if self.pending_command.is_some() {
            if self.config_enable == ConfigEnabled::No {
                self.config_begin().await
            } else if self.config_enable == ConfigEnabled::Yes {
                let a = self.pending_command.take().unwrap();
                self.process_new_command(a.0).await
            } else {
                Ok(())
            }
        } else {
            match self.command_queue.try_recv() {
                Ok(a) => {
                    if self.config_enable == ConfigEnabled::No {
                        self.prepare_command(a).await
                    } else if self.config_enable == ConfigEnabled::Yes {
                        self.process_new_command(a).await
                    } else {
                        Ok(())
                    }
                }
                _ => {
                    if self.config_enable != ConfigEnabled::No {
                        self.config_end().await
                    } else {
                        self.last_command_timestamp = None;
                        Ok(())
                    }
                }
            }
        }
    }

    async fn process_new_command(&mut self, c: protocol::CommandRequest) -> io::Result<()> {
        match c {
            protocol::CommandRequest::SetOutputNormal => {
                protocol::build_command_normal_data(&mut self.command_buffer);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::SetOutputMinimal => {
                protocol::build_command_minimal_data(&mut self.command_buffer);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::SetGeneralParameters(ref p) => {
                protocol::build_command_set_general_parameters(&mut self.command_buffer, p);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::RequestGeneralParameters(a) => {
                protocol::build_command_request_general_parameters(&mut self.command_buffer);
                let e = self.send_serial_command().await;
                if e.is_ok() {
                    self.current_command =
                        Some(protocol::CommandRequest::RequestGeneralParameters(a));
                } else {
                    a.send(Err(Error::new(ErrorKind::Other, "serial write failed!")))
                        .ok();
                }
                e
            }
            protocol::CommandRequest::RequestFirmwareVersion(a) => {
                protocol::build_command_request_version(&mut self.command_buffer);
                let e = self.send_serial_command().await;
                if e.is_ok() {
                    self.current_command =
                        Some(protocol::CommandRequest::RequestFirmwareVersion(a));
                } else {
                    a.send(Err(Error::new(ErrorKind::Other, "serial write failed!")))
                        .ok();
                }
                e
            }
            protocol::CommandRequest::RequestAutoThresholds(ref a) => {
                protocol::build_command_auto_threshold(&mut self.command_buffer, a);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::SetTriggerThresholds(ref a) => {
                protocol::build_command_set_trigger_thresholds(&mut self.command_buffer, a);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::RequestTriggerThresholds(a) => {
                protocol::build_command_request_trigger_thresholds(&mut self.command_buffer);
                let e = self.send_serial_command().await;
                if e.is_ok() {
                    self.current_command =
                        Some(protocol::CommandRequest::RequestTriggerThresholds(a));
                } else {
                    a.send(Err(Error::new(ErrorKind::Other, "serial write failed!")))
                        .ok();
                }
                e
            }
            protocol::CommandRequest::SetKeepThresholds(ref a) => {
                protocol::build_command_set_keep_thresholds(&mut self.command_buffer, a);
                self.send_serial_command().await?;
                self.current_command = Some(c);
                Ok(())
            }
            protocol::CommandRequest::RequestKeepThresholds(a) => {
                protocol::build_command_request_keep_thresholds(&mut self.command_buffer);
                let e = self.send_serial_command().await;
                if e.is_ok() {
                    self.current_command = Some(protocol::CommandRequest::RequestKeepThresholds(a));
                } else {
                    a.send(Err(Error::new(ErrorKind::Other, "serial write failed!")))
                        .ok();
                }
                e
            }
        }
    }

    async fn send_serial_command(&mut self) -> io::Result<()> {
        self.serial_stream.write(&self.command_buffer).await?;
        self.last_command_timestamp = Some(Instant::now());
        Ok(())
    }

    async fn config_begin(&mut self) -> io::Result<()> {
        self.config_enable = ConfigEnabled::Asking;
        protocol::build_command_config_begin(&mut self.command_buffer);
        self.send_serial_command().await?;
        Ok(())
    }

    async fn config_end(&mut self) -> io::Result<()> {
        self.config_enable = ConfigEnabled::No;
        protocol::build_command_config_end(&mut self.command_buffer);
        self.send_serial_command().await?;
        Ok(())
    }

    async fn prepare_command(&mut self, c: protocol::CommandRequest) -> io::Result<()> {
        self.last_command_timestamp = Some(Instant::now());
        self.pending_command = Some((c, Instant::now()));
        self.config_begin().await
    }

    fn process_command_response(&mut self, r: protocol::Response) {
        match r {
            protocol::Response::ConfigBegin(a) => {
                if a.ack == 0 {
                    self.config_enable = ConfigEnabled::Yes;
                } else {
                    warn!("config begin failed");
                    if self.config_enable != ConfigEnabled::Yes {
                        self.config_enable = ConfigEnabled::No;
                    }
                }
            }
            protocol::Response::ConfigEnd(a) => {
                if a.ack == 0 {
                    // self.config_enable = ConfigEnabled::No;
                } else {
                    warn!("config end failed");
                }
            }
            protocol::Response::FirmwreVersion(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::RequestFirmwareVersion(q)) => {
                    q.send(Ok(a)).ok();
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::SetOutputMode(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::SetOutputNormal)
                | Some(protocol::CommandRequest::SetOutputMinimal) => {
                    if a.ack != 0 {
                        warn!("set output mode command failed");
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::SetGeneralParameters(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::SetGeneralParameters(_)) => {
                    if a.ack != 0 {
                        warn!("set general parameters command failed");
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::RequestGeneralParameters(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::RequestGeneralParameters(q)) => {
                    q.send(Ok(a.into())).ok();
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::SetTriggerThresholds(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::SetTriggerThresholds(_)) => {
                    if a.ack != 0 {
                        warn!("set trigger threshold command failed");
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::RequestTriggerThresholds(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::RequestTriggerThresholds(q)) => {
                    if a.ack == 0 {
                        let b: [u32; 16] = a.thresholds.map(|x| x.into());
                        q.send(Ok(b)).ok();
                    } else {
                        q.send(Err(Error::new(ErrorKind::Other, "command ack fail")))
                            .ok();
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::SetKeepThresholds(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::SetKeepThresholds(_)) => {
                    if a.ack != 0 {
                        warn!("set keep threshold command failed");
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            protocol::Response::RequestKeepThresholds(a) => match self.current_command.take() {
                Some(protocol::CommandRequest::RequestKeepThresholds(q)) => {
                    if a.ack == 0 {
                        let b: [u32; 16] = a.thresholds.map(|x| x.into());
                        q.send(Ok(b)).ok();
                    } else {
                        q.send(Err(Error::new(ErrorKind::Other, "command ack fail")))
                            .ok();
                    }
                }
                _ => {
                    warn!("Unexpected command ack");
                }
            },
            _ => {
                warn!("Unexpected response");
            }
        }
    }

    pub fn get_cmd_sender(&self) -> Sender<protocol::CommandRequest> {
        self.command_sender.clone()
    }
}
