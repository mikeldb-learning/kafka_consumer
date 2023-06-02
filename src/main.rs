use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record};
use std::str;

fn main () {
    let hosts = vec!["localhost:9092".to_owned()];

    let mut consumer =
        Consumer::from_hosts(hosts.to_owned())
        .with_topic("globalvia.ap53.dev.camera.video.ocr".to_owned())
        .with_topic("globalvia.ap53.dev.events.fusion".to_owned())
        .with_group("background_generation_group".to_owned())
        .with_fallback_offset(FetchOffset::Latest)
        .create()
        .unwrap();

    let mut producer =
        Producer::from_hosts(hosts)
        .create()
        .unwrap();

    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                // If the consumer receives an event, this block is executed
                println!("{:?}", str::from_utf8(m.value).unwrap());
                let buf = "This is a super test";
                producer.send(&Record::from_value("globalvia.ap53.dev.events.fusion", buf.as_bytes())).unwrap();
            }

            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();
    }
}