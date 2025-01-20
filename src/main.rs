use std::time::Duration;

use amiquip::{
    Connection, ConsumerMessage, ConsumerOptions, FieldTable, QueueDeclareOptions, Result,
};
use axum::{
    Router,
    extract::{
        WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use tokio::time::interval;

fn get_data_from_rabbitmq() -> Result<()> {
    let mut connection = Connection::insecure_open("amqp://guest:guest@192.168.0.4")?;
    // .expect("Failed to connect to RabbitMQ");
    let channel = connection.open_channel(None)?;
    // .expect("Failed to open a channel");

    let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
    // .expect("Failed to declare a queue");

    channel.queue_bind("hello", "amq.direct", "hello", FieldTable::default())?;

    let consumer = queue.consume(ConsumerOptions::default())?;
    println!("Waiting for messages. Press Ctrl-C to exit.");

    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                // println!("({:>3}) Received [{}]", i, body);
                println!("({:>3}) Received ", i);
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }

    connection.close();
    Ok(())
}

async fn handle_ws(ws: WebSocketUpgrade) -> impl IntoResponse {
    println!("--> {:12} - Accessed /ws", "HANDLER");
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    let send_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let msg = "Hello from server".to_string();
            if sender.send(Message::Text(msg)).await.is_err() {
                println!("--> {:12} - Failed to send message to client", "LOGGER");
                break;
            }
            println!("--> {:12} - Sent message to client", "LOGGER");
        }
    });
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            axum::extract::ws::Message::Text(req_data) => {
                println!("--> {:12} - Received data from client", "LOGGER");
                println!("Data: {}", req_data);
            }
            axum::extract::ws::Message::Close(_) => break,
            _ => {}
        }
    }

    send_task.abort();
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    let app = Router::new().route("/ws", get(handle_ws));

    println!("--> {:12} - started running server on port 8080", "INFO");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
