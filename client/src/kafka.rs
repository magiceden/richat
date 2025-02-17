use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::error::KafkaError;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub struct KafkaProducer {
    producer: Arc<FutureProducer>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(brokers: &str, topic: &str) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Failed to create Kafka producer");

        KafkaProducer {
            producer: Arc::new(producer),
            topic: topic.to_string(),
        }
    }

    pub async fn send_message<P>(&self, key: Option<&str>, payload: P)
    where
        P: AsRef<[u8]>,
    {
        loop {
            let mut record = FutureRecord::to(&self.topic)
                .payload(payload.as_ref());

            if let Some(key) = key {
                record = record.key(key);
            }

            match self.producer.send(record, Duration::from_secs(0)).await {
                Ok(delivery) => {
                    println!("Message delivered: {:?}", delivery);
                    break;
                }
                Err((KafkaError::MessageProduction(err), _)) => {
                    eprintln!("Message production error: {:?}", err);
                    sleep(Duration::from_secs(1)).await;
                }
                Err((err, _)) => {
                    eprintln!("Failed to deliver message: {:?}", err);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub fn get_producer(&self) -> Arc<FutureProducer> {
        Arc::clone(&self.producer)
    }
}