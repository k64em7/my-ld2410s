use my_ld2410s::ld2410s::Ld2410s;
use my_ld2410s::protocol;
use tokio_serial::SerialPortBuilderExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> tokio_serial::Result<()> {
    let mut port = tokio_serial::new("/dev/ttyS0", 115200)
        .data_bits(tokio_serial::DataBits::Eight)
        .parity(tokio_serial::Parity::None)
        .stop_bits(tokio_serial::StopBits::One)
        .open_native_async()?;

    #[cfg(unix)]
    port.set_exclusive(false)
        .expect("Unable to set serial port exclusive to false");

    let mut ld2410s = Ld2410s::new(port);

    let s = ld2410s.get_cmd_sender();
    tokio::spawn(async move {
        my_func(s).await;
    });

    loop {
        let a = ld2410s.poll().await;
        println!("{:?}", a);
    }
}

async fn my_func(s: flume::Sender<protocol::CommandRequest>) {
    let (tx0, rx0) = tokio::sync::oneshot::channel();
    s.send(protocol::CommandRequest::RequestFirmwareVersion(tx0))
        .unwrap();

    let (tx1, rx1) = tokio::sync::oneshot::channel();
    s.send(protocol::CommandRequest::RequestGeneralParameters(tx1))
        .unwrap();

    let a = protocol::AutoThresholdParameters {
        trigger: 0x02,
        keep: 0x01,
        duration: 0x78,
    };
    s.send(protocol::CommandRequest::RequestAutoThresholds(a))
        .unwrap();

    match rx0.await {
        Ok(r) => match r {
            Ok(v) => println!("firmware: {:02}.{:02}.{:02}", v.major, v.minor, v.patch),
            Err(e) => println!("read firmware version failed: {}", e),
        },
        Err(_) => println!("the sender dropped"),
    }

    match rx1.await {
        Ok(r) => match r {
            Ok(v) => println!("general parameters: {:?}", v),
            Err(e) => println!("read general parameters failed: {}", e),
        },
        Err(_) => println!("the sender dropped"),
    }
}
