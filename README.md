Bot discord used to publish message on Twitch or Youtube event.

```mermaid
flowchart LR
    webhook[Webhook Gateway]
    discord_consumer[Discord Consumer]
    Youtube -->|webhook| webhook
    Twitch -->|webhook| webhook
    webhook --> |Publish in topic| Pulsar --> |Read in topic| discord_consumer --> Discord
```
