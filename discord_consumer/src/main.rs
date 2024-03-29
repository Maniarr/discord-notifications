use std::env;

use serenity::{
    async_trait,
    model::{channel::{Message as DiscordMessage, ChannelType}, gateway::Ready},
    prelude::*,
};
use serenity::model::id::ChannelId;

use futures::stream::StreamExt;

use serde::{Deserialize, Serialize};
use serde_json::{
    json,
    Value as JsonValue,
};
use std::collections::HashMap;

use handlebars::Handlebars;
use futures::TryStreamExt;
use pulsar::{
    Producer,
    message::proto::command_subscribe::SubType, message::Payload, Consumer, consumer::{ ConsumerOptions, InitialPosition }, DeserializeMessage,
    Pulsar, TokioExecutor, SerializeMessage, Error as PulsarError, producer, MultiTopicProducer,
}; 

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
#[serde(untagged)]
enum MessageEvent {
    Twitch(EventMessage),
    Youtube(EventYoutubeVideo)
}

#[derive(Debug, Deserialize, Serialize)]
struct Handler {
    youtube: HashMap<String, HashMap<u64, String>>,
    twitch: HashMap<String, HashMap<String, HashMap<u64, String>>>,
}

impl DeserializeMessage for MessageEvent {
    type Output = Result<MessageEvent, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: Ready) {
        log::info!("{} is connected!", ready.user.name);

        let pulsar: Pulsar<_> = Pulsar::builder(env::var("PULSAR_URL").expect("PULSAR_URL variable not provided"), TokioExecutor).build().await.expect("Failed to build pulsar client");

        let topics_with_comma = env::var("TOPICS_LISTEN").expect("TOPICS_LISTEN variable not provided");

        let topics: Vec<&str> = topics_with_comma
            .split(',')
            .collect();

        let mut consumer: Consumer<MessageEvent, _> = pulsar
            .consumer()
            .with_topics(topics)
            .with_consumer_name(env::var("CONSUMER_NAME").expect("CONSUMER_NAME variable not provided"))
            .with_subscription_type(SubType::Shared)
            .with_subscription(env::var("CONSUMER_NAME").expect("CONSUMER_NAME variable not provided"))
            .with_batch_size(10)
            .build()
            .await
            .expect("Failed to build pulsar consumer");

        let mut cache: HashMap<String, bool> = HashMap::new();

        while let Some(message) = consumer.try_next().await.expect("Failed to consume message") {
            log::info!("new event from pulsar");
        
            match message.deserialize() {
                Ok(MessageEvent::Youtube(video)) => {
                    log::info!("Youtube event");
                    println!("{:?}", &video);

                    if !cache.contains_key(&video.id) {
                        if let Some(entries) = self.youtube.get(video.channel_id.as_str()) {
                            let mut handlebars = Handlebars::new();
                            handlebars.register_escape_fn(handlebars::no_escape);
                                
                            for (channel_id, message_format) in entries {
                                ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"video_link": video.link})).unwrap()).await;
                            }

                            dbg!(consumer.ack(&message).await);

                            cache.insert(video.id.clone(),true);

                            log::info!("Message sended to discord");
                        } else {
                            log::info!("Channel id not found in youtube mapping {}", &video.channel_id);
                        }
                    } else {
                         log::info!("Video in cache, skip the message");
                    }
                },
                Ok(MessageEvent::Twitch(event)) => {
                    log::info!("Twitch stream online event");
                    log::debug!("{:?}", &event);

                    match event.event {
                        TwitchEvent::StreamOnline { broadcaster_user_id, broadcaster_user_name, broadcaster_user_login, started_at, ..} => {
                            if let Some(online_mapping) = self.twitch.get(&event.event_type) {
                                if let Some(entries) = online_mapping.get(&broadcaster_user_id) {
                                    let mut handlebars = Handlebars::new();
                                    handlebars.register_escape_fn(handlebars::no_escape);
                                                
                                    for (channel_id, message_format) in entries {
                                        match ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"broadcaster_name": broadcaster_user_name, "broadcaster_login": broadcaster_user_login})).unwrap()).await {
                                            Ok(message) => {
                                                log::info!("{:?}", &message);

                                                // We need to trigger a crosspost message in the news channels so that it is sent to all of the follower channels.
                                                // if let Ok(channel) = ChannelId(channel_id.clone()).to_channel(&ctx.http).await {
                                                //     if let Some(guild_channel) = channel.guild() {
                                                //         if ChannelType::News == guild_channel.kind {
                                                //             message.crosspost(&ctx.http).await;
                                                //         }
                                                //     }
                                                // }
                                            },
                                            Err(error) => {
                                                log::error!("{}", error);
                                            }
                                        };
                                    }
                                                
                                    dbg!(consumer.ack(&message).await);

                                    log::info!("Message sended to discord");
                                }
                            }
                        },
                        TwitchEvent::StreamOffline { broadcaster_user_id, broadcaster_user_name, broadcaster_user_login, .. } => {
                            log::info!("Twitch stream offline event");

                            if let Some(online_mapping) = self.twitch.get(&event.event_type) {
                                if let Some(entries) = online_mapping.get(&broadcaster_user_id) {
                                    let mut handlebars = Handlebars::new();
                                    handlebars.register_escape_fn(handlebars::no_escape);
                                    
                                    for (channel_id, message_format) in entries {
                                        ChannelId(channel_id.clone()).say(&ctx.http, handlebars.render_template(message_format, &json!({"broadcaster_name": broadcaster_user_name})).unwrap()).await;
                                    }

                                    dbg!(consumer.ack(&message).await);

                                    log::info!("Message sended to discord");
                                }
                            }
                        },
                    }
                },
                Err(error) => {
                    log::error!("{}", error);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "discord=info");
    env_logger::init();
    
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");

    let config_content = std::fs::read_to_string("config.yml").expect("Failed to read config.yml");

    let handler: Handler = serde_yaml::from_str(&config_content).expect("Not valid yaml inconfig.yml");

    let mut client = Client::builder(&token).event_handler(handler).await.expect("Err creating client");
    
    if let Err(why) = client.start().await {
        log::error!("Client error: {:?}", why);
    }
}
