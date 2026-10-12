use log::warn;
use std::io::{Error, ErrorKind};
use tokio::sync::oneshot;
use zerocopy::{
    IntoBytes, TryFromBytes,
    byteorder::little_endian::{U16, U32},
};
use zerocopy_derive::TryFromBytes;

const FRAME_HEAD_MINIMAL: [u8; 1] = [0x6E];
const FRAME_FOOT_MINIMAL: [u8; 1] = [0x62];
const FRAME_HEAD_DATA: [u8; 4] = [0xF4, 0xF3, 0xF2, 0xF1];
const FRAME_FOOT_DATA: [u8; 4] = [0xF8, 0xF7, 0xF6, 0xF5];
const FRAME_HEAD_CMD: [u8; 4] = [0xFD, 0xFC, 0xFB, 0xFA];
const FRAME_FOOT_CMD: [u8; 4] = [0x04, 0x03, 0x02, 0x01];

const CMD_NORMAL_DATA: [u8; 8] = [0x7A, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00];
const CMD_MINIMAL_DATA: [u8; 8] = [0x7A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

const CONFIG_BEGIN_DATA: [u8; 4] = [0xFF, 0x00, 0x01, 0x00];
const CONFIG_END_DATA: [u8; 4] = [0xFE, 0x00, 0x01, 0x00];

const CMD_REQUEST_VERSION_DATA: [u8; 2] = [0x00, 0x00];

const CMD_REQUEST_GENERAL_PARAMETERS_DATA: [u8; 14] = [
    0x71, 0x00, 0x05, 0x00, 0x0A, 0x00, 0x06, 0x00, 0x02, 0x00, 0x0C, 0x00, 0x0B, 0x00,
];

const CMD_REQUEST_TRIGGER_THRESHOLDS_DATA: [u8; 34] = [
    0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x05, 0x00, 0x06, 0x00,
    0x07, 0x00, 0x08, 0x00, 0x09, 0x00, 0x0A, 0x00, 0x0B, 0x00, 0x0C, 0x00, 0x0D, 0x00, 0x0E, 0x00,
    0x0F, 0x00,
];

const CMD_REQUEST_KEEP_THRESHOLDS_DATA: [u8; 34] = [
    0x77, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x05, 0x00, 0x06, 0x00,
    0x07, 0x00, 0x08, 0x00, 0x09, 0x00, 0x0A, 0x00, 0x0B, 0x00, 0x0C, 0x00, 0x0D, 0x00, 0x0E, 0x00,
    0x0F, 0x00,
];

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
pub enum RespondSpeed {
    Normal = 5,
    Fast = 10,
}

#[derive(Debug)]
pub struct GeneralParameters {
    pub max_threshold: u8,    // 1 to 16
    pub min_threshold: u8,    // 1 to 16
    pub empty_delay: u8,      // 10 to 120
    pub report_frequency: u8, // 5 to 80; step 5
    pub respond_speed: RespondSpeed,
}

pub struct AutoThresholdParameters {
    pub trigger: u16,
    pub keep: u16,
    pub duration: u16,
}

#[derive(Debug, PartialEq)]
enum DecoderState {
    Head,
    HeadData(u8),
    HeadCmd(u8),
    DataLen(u8, u8),
    Data(u16),
    FootMinimal,
    FootData(u8),
    FootCmd(u8),
}

enum FrameKind {
    MinimalData,
    Data,
    CommandAck,
    None,
}

pub struct FrameDecoder {
    state: DecoderState,
    kind: FrameKind,
    buf: Vec<u8>,
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct MinimalData {
    pub target_status: u8,
    pub distance: U16,
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct NormalData {
    pub target_status: u8,
    pub distance: U16,
    pub padding: U16,
    pub threshold_energy: [U32; 16],
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct AutoThresholdData {
    pub progress: U16,
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct FirmwareVersionResponse {
    unknown: [u8; 6],
    pub major: U16,
    pub minor: U16,
    pub patch: U16,
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct ConfigBeginResponse {
    pub ack: U16,
    pub protocol: U16,
    pub buffer_size: U16,
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct RequestGeneralParametersResponse {
    pub ack: U16,
    pub max_threshold: U32,           // 1 to 16
    pub min_threshold: U32,           // 1 to 16
    pub empty_delay: U32,             // 10 to 120
    pub target_report_frequency: U32, // 5 to 80
    pub distance_frequency: U32,      // 5 to 80
    pub respond_speed: U32,
}

impl From<RequestGeneralParametersResponse> for GeneralParameters {
    fn from(value: RequestGeneralParametersResponse) -> Self {
        Self {
            max_threshold: value.max_threshold.to_bytes()[0],
            min_threshold: value.min_threshold.to_bytes()[0],
            empty_delay: value.empty_delay.to_bytes()[0],
            report_frequency: value.target_report_frequency.to_bytes()[0],
            respond_speed: if value.respond_speed == 5 {
                RespondSpeed::Normal
            } else {
                RespondSpeed::Fast
            },
        }
    }
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct RequestThresholdsResponse {
    pub ack: U16,
    pub thresholds: [U32; 16],
}

#[derive(TryFromBytes)]
#[repr(C)]
pub struct CommandResponse {
    pub ack: U16,
}

pub enum CommandRequest {
    SetOutputNormal,
    SetOutputMinimal,
    SetGeneralParameters(GeneralParameters),
    RequestGeneralParameters(oneshot::Sender<Result<GeneralParameters, Error>>),
    RequestFirmwareVersion(oneshot::Sender<Result<FirmwareVersionResponse, Error>>),
    RequestAutoThresholds(AutoThresholdParameters),
    SetTriggerThresholds([u32; 16]),
    RequestTriggerThresholds(oneshot::Sender<Result<[u32; 16], Error>>),
    SetKeepThresholds([u32; 16]),
    RequestKeepThresholds(oneshot::Sender<Result<[u32; 16], Error>>),
}

pub enum Response {
    DataNormal(NormalData),
    DataMinimal(MinimalData),
    DataAutoThreshold(AutoThresholdData),
    FirmwreVersion(FirmwareVersionResponse),
    ConfigBegin(ConfigBeginResponse),
    ConfigEnd(CommandResponse),
    SetOutputMode(CommandResponse),
    SetGeneralParameters(CommandResponse),
    RequestGeneralParameters(RequestGeneralParametersResponse),
    SetTriggerThresholds(CommandResponse),
    RequestTriggerThresholds(RequestThresholdsResponse),
    SetKeepThresholds(CommandResponse),
    RequestKeepThresholds(RequestThresholdsResponse),
    ResponseUnknown(u16, Vec<u8>),
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            state: DecoderState::Head,
            kind: FrameKind::None,
            buf: Vec::with_capacity(139),
        }
    }

    pub fn feed(&mut self, u: u8) -> Option<Result<Response, Error>> {
        match self.state {
            DecoderState::Head => match u {
                x if x == FRAME_HEAD_MINIMAL[0] => {
                    self.kind = FrameKind::MinimalData;
                    self.state = DecoderState::Data(3);
                    None
                }
                x if x == FRAME_HEAD_DATA[0] => {
                    self.kind = FrameKind::Data;
                    self.state = DecoderState::HeadData(1);
                    None
                }
                x if x == FRAME_HEAD_CMD[0] => {
                    self.kind = FrameKind::CommandAck;
                    self.state = DecoderState::HeadCmd(1);
                    None
                }
                _ => None,
            },
            DecoderState::HeadData(mut n) => {
                if u == FRAME_HEAD_DATA[n as usize] {
                    n += 1;
                    if n < 4 {
                        self.state = DecoderState::HeadData(n);
                    } else {
                        self.state = DecoderState::DataLen(0, 0);
                    }
                    None
                } else {
                    // Invalid delim
                    self.reset();
                    self.feed(u)
                }
            }
            DecoderState::HeadCmd(mut n) => {
                if u == FRAME_HEAD_CMD[n as usize] {
                    n += 1;
                    if n < 4 {
                        self.state = DecoderState::HeadCmd(n);
                    } else {
                        self.state = DecoderState::DataLen(0, 0);
                    }
                    None
                } else {
                    // Invalid delim
                    self.reset();
                    self.feed(u)
                }
            }
            DecoderState::DataLen(m, n) => {
                if m == 0 {
                    self.state = DecoderState::DataLen(1, u);
                    None
                } else {
                    let l = u16::from_le_bytes([n, u]);
                    if (l > 0) && (l < 160) {
                        self.state = DecoderState::Data(l);
                        None
                    } else {
                        // Invalid len
                        self.reset();
                        self.feed(u)
                    }
                }
            }
            DecoderState::Data(mut n) => {
                self.buf.push(u);
                n -= 1;
                if n > 0 {
                    self.state = DecoderState::Data(n);
                } else {
                    match self.kind {
                        FrameKind::MinimalData => {
                            self.state = DecoderState::FootMinimal;
                        }
                        FrameKind::Data => {
                            self.state = DecoderState::FootData(0);
                        }
                        FrameKind::CommandAck => {
                            self.state = DecoderState::FootCmd(0);
                        }
                        FrameKind::None => {
                            unreachable!();
                        }
                    }
                }
                None
            }
            DecoderState::FootMinimal => {
                if u == FRAME_FOOT_MINIMAL[0] {
                    Some(self.process_data())
                } else {
                    // invalid delim
                    self.reset();
                    self.feed(u)
                }
            }
            DecoderState::FootData(mut n) => {
                if u == FRAME_FOOT_DATA[n as usize] {
                    n += 1;
                    if n < 4 {
                        self.state = DecoderState::FootData(n);
                        None
                    } else {
                        Some(self.process_data())
                    }
                } else {
                    // Invalid delim
                    self.reset();
                    self.feed(u)
                }
            }
            DecoderState::FootCmd(mut n) => {
                if u == FRAME_FOOT_CMD[n as usize] {
                    n += 1;
                    if n < 4 {
                        self.state = DecoderState::FootCmd(n);
                        None
                    } else {
                        Some(self.process_data())
                    }
                } else {
                    // Invalid delim
                    self.reset();
                    self.feed(u)
                }
            }
        }
    }

    pub fn reset(&mut self) {
        self.state = DecoderState::Head;
        self.kind = FrameKind::None;
        self.buf.clear();
    }

    fn process_data(&mut self) -> Result<Response, Error> {
        let r = match self.kind {
            FrameKind::MinimalData => MinimalData::try_read_from_bytes(&self.buf).map_or_else(
                |_| {
                    Err(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid data (MinimalData)",
                    ))
                },
                |x| Ok(Response::DataMinimal(x)),
            ),
            FrameKind::Data => {
                if self.buf.len() > 0 {
                    match self.buf[0] {
                        0x01 => NormalData::try_read_from_bytes(&self.buf[1..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (NormalData)",
                                ))
                            },
                            |x| Ok(Response::DataNormal(x)),
                        ),
                        0x03 => AutoThresholdData::try_read_from_bytes(&self.buf[1..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (AutoThresholdData)",
                                ))
                            },
                            |x| Ok(Response::DataAutoThreshold(x)),
                        ),
                        _ => Err(Error::new(
                            ErrorKind::InvalidData,
                            "Invalid data (Data type is invalid)",
                        )),
                    }
                } else {
                    Err(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid data (Data is empty)",
                    ))
                }
            }
            FrameKind::CommandAck => {
                if self.buf.len() < 2 {
                    Err(Error::new(
                        ErrorKind::InvalidData,
                        "Invalid data (CommandAck)",
                    ))
                } else {
                    // command word
                    match u16::from_le_bytes((&self.buf[..2]).try_into().unwrap()) {
                        0x17A => CommandResponse::try_read_from_bytes(&self.buf[2..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (CommandResponse)",
                                ))
                            },
                            |x| Ok(Response::SetOutputMode(x)),
                        ),
                        0x100 => {
                            warn!(
                                "The firmware version response is not fully understood: {:?}",
                                &self.buf[2..]
                            );
                            FirmwareVersionResponse::try_read_from_bytes(&self.buf[2..])
                                .map_or_else(
                                    |_| {
                                        Err(Error::new(
                                            ErrorKind::InvalidData,
                                            "Invalid data (FirmwareVersionResponse)",
                                        ))
                                    },
                                    |x| Ok(Response::FirmwreVersion(x)),
                                )
                        }
                        0x1FF => ConfigBeginResponse::try_read_from_bytes(&self.buf[2..])
                            .map_or_else(
                                |_| {
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        "Invalid data (ConfigBeginResponse)",
                                    ))
                                },
                                |x| Ok(Response::ConfigBegin(x)),
                            ),
                        0x1FE => CommandResponse::try_read_from_bytes(&self.buf[2..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (CommandResponse)",
                                ))
                            },
                            |x| Ok(Response::ConfigEnd(x)),
                        ),
                        0x170 => CommandResponse::try_read_from_bytes(&self.buf[2..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (CommandResponse)",
                                ))
                            },
                            |x| Ok(Response::SetGeneralParameters(x)),
                        ),
                        0x171 => {
                            RequestGeneralParametersResponse::try_read_from_bytes(&self.buf[2..])
                                .map_or_else(
                                    |_| {
                                        Err(Error::new(
                                            ErrorKind::InvalidData,
                                            "Invalid data (RequestGeneralParametersResponse)",
                                        ))
                                    },
                                    |x| Ok(Response::RequestGeneralParameters(x)),
                                )
                        }
                        0x173 => RequestThresholdsResponse::try_read_from_bytes(&self.buf[2..])
                            .map_or_else(
                                |_| {
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        "Invalid data (RequestThresholdsResponse)",
                                    ))
                                },
                                |x| Ok(Response::RequestTriggerThresholds(x)),
                            ),
                        0x177 => RequestThresholdsResponse::try_read_from_bytes(&self.buf[2..])
                            .map_or_else(
                                |_| {
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        "Invalid data (RequestThresholdsResponse)",
                                    ))
                                },
                                |x| Ok(Response::RequestKeepThresholds(x)),
                            ),
                        0x172 => CommandResponse::try_read_from_bytes(&self.buf[2..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (CommandResponse)",
                                ))
                            },
                            |x| Ok(Response::SetTriggerThresholds(x)),
                        ),
                        0x176 => CommandResponse::try_read_from_bytes(&self.buf[2..]).map_or_else(
                            |_| {
                                Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Invalid data (CommandResponse)",
                                ))
                            },
                            |x| Ok(Response::SetKeepThresholds(x)),
                        ),
                        x => {
                            let v1 = self.buf.split_off(2);
                            Ok(Response::ResponseUnknown(x, v1))
                        }
                    }
                }
            }
            FrameKind::None => {
                unreachable!();
            }
        };
        self.reset();
        r
    }
}

fn build_command(buf: &mut Vec<u8>, data: &[u8]) {
    let data_len = U16::new(data.len() as u16).to_bytes();
    buf.clear();
    buf.extend_from_slice(&FRAME_HEAD_CMD);
    buf.extend_from_slice(&data_len);
    buf.extend_from_slice(data);
    buf.extend_from_slice(&FRAME_FOOT_CMD);
}

pub fn build_command_set_general_parameters(buf: &mut Vec<u8>, p: &GeneralParameters) {
    let mut v = Vec::<u8>::new();
    v.extend_from_slice(&[0x70, 0x00]);
    v.extend_from_slice(&[0x05, 0x00]);
    v.extend_from_slice(&U32::new(p.max_threshold as u32).to_bytes());
    v.extend_from_slice(&[0x0A, 0x00]);
    v.extend_from_slice(&U32::new(p.min_threshold as u32).to_bytes());
    v.extend_from_slice(&[0x06, 0x00]);
    v.extend_from_slice(&U32::new(p.empty_delay as u32).to_bytes());
    v.extend_from_slice(&[0x02, 0x00]);
    v.extend_from_slice(&U32::new(p.report_frequency as u32).to_bytes());
    v.extend_from_slice(&[0x0C, 0x00]);
    v.extend_from_slice(&U32::new(p.report_frequency as u32).to_bytes());
    v.extend_from_slice(&[0x0B, 0x00]);
    v.extend_from_slice(&U32::new(p.respond_speed as u32).to_bytes());
    build_command(buf, &v);
}

pub fn build_command_auto_threshold(buf: &mut Vec<u8>, p: &AutoThresholdParameters) {
    let mut v = Vec::<u8>::new();
    v.extend_from_slice(&[0x09, 0x00]);
    v.extend_from_slice(&U16::new(p.trigger).to_bytes());
    v.extend_from_slice(&U16::new(p.keep).to_bytes());
    v.extend_from_slice(&U16::new(p.duration).to_bytes());
    build_command(buf, &v);
}

pub fn build_command_normal_data(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_NORMAL_DATA);
}

pub fn build_command_minimal_data(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_MINIMAL_DATA);
}

pub fn build_command_request_version(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_REQUEST_VERSION_DATA);
}

pub fn build_command_request_general_parameters(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_REQUEST_GENERAL_PARAMETERS_DATA);
}
pub fn build_command_request_trigger_thresholds(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_REQUEST_TRIGGER_THRESHOLDS_DATA);
}

pub fn build_command_request_keep_thresholds(buf: &mut Vec<u8>) {
    build_command(buf, &CMD_REQUEST_KEEP_THRESHOLDS_DATA);
}

pub fn build_command_set_trigger_thresholds(buf: &mut Vec<u8>, a: &[u32; 16]) {
    let mut v = Vec::<u8>::new();
    v.extend_from_slice(&[0x72, 0x00]);
    for i in 0..16 {
        v.extend_from_slice(&[i as u8, 0x00]);
        v.extend_from_slice(U32::new(a[i]).as_bytes());
    }
    build_command(buf, &v);
}

pub fn build_command_set_keep_thresholds(buf: &mut Vec<u8>, a: &[u32; 16]) {
    let mut v = Vec::<u8>::new();
    v.extend_from_slice(&[0x76, 0x00]);
    for i in 0..16 {
        v.extend_from_slice(&[i as u8, 0x00]);
        v.extend_from_slice(U32::new(a[i]).as_bytes());
    }
    build_command(buf, &v);
}

pub fn build_command_config_begin(buf: &mut Vec<u8>) {
    build_command(buf, &CONFIG_BEGIN_DATA);
}

pub fn build_command_config_end(buf: &mut Vec<u8>) {
    build_command(buf, &CONFIG_END_DATA);
}
