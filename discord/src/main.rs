use std::env;

use serenity::{
    async_trait,
    model::{channel::Message as DiscordMessage, gateway::Ready},
    prelude::*,
};
use serenity::model::id::ChannelId;

use futures::stream::StreamExt;

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::message::Message;

use serde::{Deserialize, Serialize};
use serde_json::{
    json,
    Value as JsonValue,
};
use std::collections::HashMap;

use handlebars::Handlebars;

#[derive(Debug, Deserialize, Serialize)]
struct EventMessage {
    #[serde(rename="type")]
    event_type: String,
    event: TwitchEvent,
    triggered_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum TwitchEvent {
    StreamOnline {
        id: String,
        broadcaster_user_id: String,
        broadcaster_user_login: String,
        broadcaster_user_name: String,
        #[serde(rename="type")]
        stream_type: String,
        started_at: chrono::DateTime<chrono::Utc>,
    },
    StreamOffline {
        broadcaster_user_id: String,
        broadcaster_user_login: String,
        broadcaster_user_name: String,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Author {
    name: String,
    uri: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct EventYoutubeVideo {
    id: String,
    channel_id: String,
    title: String,
    link: String,
    author: Author,
    published_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>
}

#[derive(Debug, Deserialize, Serialize)]
struct KafkaConfiguration {
    broker_ip: String,
    topics: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Handler {
    youtube: HashMap<String, HashMap<u64, String>>,
    twitch: HashMap<String, HashMap<String, HashMap<u64, String>>>,
    kafka: KafkaConfiguration,
}

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: DiscordMessage) {
        if msg.content == "!ping" {
            if let Err(why) = msg.channel_id.say(&ctx.http, "Pong!").await {
                println!("Error sending message: {:?}", why);
            }
        }
    }

    async fn ready(&self, ctx: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "discord")
            .set("bootstrap.servers", &self.kafka.broker_ip)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&self.kafka.topics.iter().map(|i| i.as_str()).collect::<Vec<&str>>())
            .expect("Can't subscribe to specified topics");

        let mut cache: HashMap<String, bool> = HashMap::new();

        let mut stream = consumer.stream();

        loop {
            match stream.next().await {
                Some(Err(e)) => println!("Kafka error: {}", e),
                Some(Ok(m)) => {
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            println!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };

                    if m.topic() == "twitch" {
                        match serde_json::from_str::<EventMessage>(payload) {
                            Ok(event) => {
                                dbg!(&event);
                                match event.event {
                                    TwitchEvent::StreamOnline { broadcaster_user_id, broadcaster_user_name, broadcaster_user_login, started_at, ..} => {
                                        if let Some(online_mapping) = self.twitch.get(&event.event_type) {
                                            if let Some(entries) = online_mapping.get(&broadcaster_user_id) {
                                                let mut handlebars = Handlebars::new();
                                                handlebars.register_escape_fn(handlebars::no_escape);
                                                
                                                for (channel_id, message_format) in entries {
                                                    ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"broadcaster_name": broadcaster_user_name, "broadcaster_login": broadcaster_user_login})).unwrap()).await;
                                                }
                                    
                                            }
                                        }
                                    },
                                    TwitchEvent::StreamOffline { broadcaster_user_id, broadcaster_user_name, broadcaster_user_login, .. } => {
                                        if let Some(online_mapping) = self.twitch.get(&event.event_type) {
                                            if let Some(entries) = online_mapping.get(&broadcaster_user_id) {
                                                let mut handlebars = Handlebars::new();
                                                handlebars.register_escape_fn(handlebars::no_escape);
                                                
                                                for (channel_id, message_format) in entries {
                                                    ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"broadcaster_name": broadcaster_user_name})).unwrap()).await;
                                                }
                                            }
                                        }
                                    },
                                };
                            },
                            Err(error) => {

                            }
                        };
                    } else {
                        match serde_json::from_str::<EventYoutubeVideo>(payload) {
                            Ok(video) => {
                                if !cache.contains_key(&video.id) {
                                    if let Some(entries) = self.youtube.get(video.channel_id.as_str()) {
                                        let mut handlebars = Handlebars::new();
                                        handlebars.register_escape_fn(handlebars::no_escape);
                                        
                                        for (channel_id, message_format) in entries {
                                            ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"video_link": video.link})).unwrap()).await;
                                        }

                                        cache.insert(video.id.clone(),true);
                                    } else {
                                        println!("Channel id not found in youtube mapping {}", &video.channel_id);
                                    }
                                } else {
                                    println!("Video in cache, skip the message");
                                }
                            },
                            Err(error) => {

                            }
                        }
                    }

                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                },
                _ => {
                    println!("nope");
                }
            };
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");

    let config_content = std::fs::read_to_string("config.yml").expect("Failed to read config.yml");

    let handler: Handler = serde_yaml::from_str(&config_content).expect("Not valid yaml inconfig.yml");

    let mut client = Client::builder(&token).event_handler(handler).await.expect("Err creating client");
    
    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    }
}
