use actix_web::{post, get, web, App, HttpServer, Responder, HttpRequest, web::Bytes, HttpResponse, http::HeaderMap};

use serde_json::{
    self,
    json,
    Value as JsonValue,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use hmac::{Hmac, Mac, NewMac};

use std::sync::Arc;

use std::time::Duration;
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

use std::env;

use actix_web::{middleware::Logger};

use r2d2_redis::{r2d2, RedisConnectionManager};
use r2d2_redis::redis::Commands;

#[derive(Debug, Deserialize)]
struct TwitchTransport {
    method: String,
    callback: String,
}

#[derive(Debug, Deserialize)]
struct TwitchSubscription {
    id: String,
    #[serde(rename="type")]
    event_type: String,
    version: String,
    status: String,
    cost: i64,
    condition: JsonValue,
    transport: TwitchTransport,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct TwitchCallback {
    challenge: Option<String>,
    subscription: TwitchSubscription,
    event: Option<JsonValue>,
}

#[derive(Debug, Serialize)]
struct EventMessage {
    #[serde(rename="type")]
    event_type: String,
    event: JsonValue,
    triggered_at: chrono::DateTime<chrono::Utc>,
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

impl From<&YoutubeVideo> for EventYoutubeVideo {
    fn from(video: &YoutubeVideo) -> Self {
        Self {
            id: video.id.clone(),
            channel_id: video.channel_id.clone(),
            title: video.title.clone(),
            link: video.link.href.clone(),
            author: video.author.clone(),
            published_at: video.published_at,
            updated_at: video.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct YoutubeQuery {
    #[serde(rename="hub.challenge")]
    challenge: String,
    #[serde(rename="hub.lease_seconds")]
    lease_seconds: i64,
    #[serde(rename="hub.mode")]
    mode: String,
    #[serde(rename="hub.topic")]
    topic: String,
    #[serde(rename="hub.verify_token")]
    verify_token: Option<String>
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Author {
    name: String,
    uri: String,
}

#[derive(Debug, Deserialize)]
struct Link {
    href: String,
}

#[derive(Debug, Deserialize)]
struct YoutubeVideo {
    #[serde(rename="videoId")]
    id: String,
    #[serde(rename="channelId")]
    channel_id: String,
    title: String,
    link: Link,
    #[serde(rename="author")]
    author: Author,
    #[serde(rename="published")]
    published_at:  chrono::DateTime<chrono::Utc>,
    #[serde(rename="updated")]
    updated_at:  chrono::DateTime<chrono::Utc>
}

#[derive(Debug, Deserialize)]
struct YoutubeFeed {
    #[serde(rename = "entry", default)]
    entries: Vec<YoutubeVideo>,
}

fn verify_twitch_signature(headers: &HeaderMap, body: &Bytes, secret: &str) -> bool {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");

    if let (Some(id), Some(timestamp), Some(req_signature)) = (headers.get("Twitch-Eventsub-Message-Id"), headers.get("Twitch-Eventsub-Message-Timestamp"), headers.get("Twitch-Eventsub-Message-Signature")) {
        mac.update(id.as_bytes());
        mac.update(timestamp.as_bytes());
        mac.update(&body);
        
        return req_signature.to_str().unwrap() == format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    }

    false
}

fn verify_youtube_signature(headers: &HeaderMap, body: &Bytes, secret: &[u8]) -> bool {
    if let Some(req_signature) = headers.get("X-Hub-Signature") {
        return 
            req_signature.to_str().unwrap() == 
            format!("sha1={}", hex::encode(hmacsha1::hmac_sha1(secret, &body)));
    }

    false
}

#[get("/json")]
async fn test() -> impl Responder {
    return HttpResponse::Ok()
        .header("Bloom-Response-Buckets", "page:donations")
        .json(json!({
            "name": "test",
        }));
}

#[get("/webhooks/youtube")]
async fn verify_youtube_webhook(youtube_query: web::Query<YoutubeQuery>, app: web::Data<YoutubeApp>) -> impl Responder {
    let youtube_query = youtube_query.into_inner();

    if youtube_query.verify_token == app.verify_token {
        return HttpResponse::Ok().body(youtube_query.challenge);
    }

    HttpResponse::Unauthorized().finish()
}

#[post("/webhooks/youtube")]
async fn youtube_webhook(req: HttpRequest, body: Bytes, kafka: web::Data<Arc<FutureProducer>>, redis: web::Data<r2d2::Pool<RedisConnectionManager>>, app: web::Data<YoutubeApp>) -> impl Responder {
    if !verify_youtube_signature(req.headers(), &body, app.hmac_secret.as_bytes()) {
        return HttpResponse::Unauthorized().finish();
    }

    let mut redis = redis.into_inner().get().unwrap();

    if let Ok(youtube_feed) = quick_xml::de::from_str::<YoutubeFeed>(std::str::from_utf8(&body).unwrap()) {
        let producer = kafka.into_inner();
        
        for video in youtube_feed.entries {
            let cache: Option<String> = redis.get(&video.id).unwrap();

            if cache.is_none() {
                match producer.send(
                    FutureRecord::to("youtube")
                            .payload(&serde_json::to_string(&EventYoutubeVideo::from(&video)).unwrap())
                            .key(&video.id),
                Duration::from_secs(2),
                ).await {
                    Ok(_) => {
                        let _: () = redis.set(&video.id, format!("{}", &video.published_at)).unwrap();
                    },
                    Err(error) => {
                        dbg!(error);
                    }
                };
            } else {
                println!("video id {} already handled", &video.id);
            }
        }

        HttpResponse::Ok().finish()
    } else {
        HttpResponse::NotAcceptable().finish()
    }
}


#[post("/webhooks/twitch")]
async fn twitch_webhook(req: HttpRequest, body: Bytes, kafka: web::Data<Arc<FutureProducer>>, twitch: web::Data<TwitchApp>) -> impl Responder {
    if !verify_twitch_signature(req.headers(), &body, &twitch.hmac_secret) {
        return HttpResponse::Unauthorized().finish();
    }
   
    if let Ok(twitch_callback) = serde_json::from_slice::<TwitchCallback>(&body) {
        if let Some(challenge) = twitch_callback.challenge {
            return HttpResponse::Ok().body(challenge);
        }

        let producer = kafka.into_inner();

        match dbg!(producer.send(
            FutureRecord::to("twitch")
                .payload(&serde_json::to_string(&EventMessage {
                    event_type: twitch_callback.subscription.event_type,
                    event: twitch_callback.event.unwrap(),
                    triggered_at: chrono::Utc::now(),
                }).unwrap())
                .key(""),
            Duration::from_secs(2),
        ).await) {
            Ok(_) => {

            },
            Err(error) => {
                dbg!(error);
            }
        };

        return HttpResponse::Ok().finish();
    } else {
        return HttpResponse::InternalServerError().finish();
    }
}

#[derive(Debug)]
struct YoutubeApp {
    verify_token: Option<String>,
    hmac_secret: String,
}

#[derive(Debug)]
struct TwitchApp {
    hmac_secret: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();

    HttpServer::new(|| {
        let verify_token = match env::var("YOUTUBE_VERIFY_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => None,
        };

        let manager = RedisConnectionManager::new(env::var("REDIS_URL").expect("REDIS_URL not in environment")).unwrap();
        let pool = r2d2::Pool::builder()
            .build(manager)
            .expect("Failed to build redis pool");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", env::var("BROKER_IP").expect("BROKER_IP not in environment"))
            .set("message.timeout.ms", "2000")
            .create()
            .expect("Producer creation error");
    
        App::new()
            .wrap(Logger::default())
            .data( Arc::new(producer))
            .data(YoutubeApp {
                verify_token: verify_token.clone(),
                hmac_secret: env::var("YOUTUBE_HMAC_SECRET").expect("YOUTUBE_HMAC_SECRET not in environment"),
            })
            .data(TwitchApp {
                hmac_secret: env::var("TWITCH_HMAC_SECRET").expect("TWITCH_HMAC_SECRET not in environment"),
            })
            .data(pool)
            .service(twitch_webhook)
            .service(verify_youtube_webhook)
            .service(youtube_webhook)
            .service(test)
    })
        .bind(env::var("LISTEN_ADDRESS").expect("LISTEN_ADDRESS not in environment"))?
        .run()
        .await
}
