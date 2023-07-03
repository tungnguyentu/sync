from environs import Env

env = Env()
env.read_env()

host = env.str("HOST")

#kafka
bootstrap_servers = env.list("BOOTSTRAP_SERVERS")
kafka_ssl_context = env.bool("KAFKA_SSL_CONTEXT", False)
event_topic = env.str("EVENT_TOPIC")
sasl_plain_username = env.str("SASL_PLAIN_USERNAME", None)
sasl_plain_password = env.str("SASL_PLAIN_PASSWORD", None)
security_protocol = env.str("SECURITY_PROTOCOL", None)
sasl_mechanism = env.str("SASL_MECHANISM", None)

tele_chat_id = env.str("CHAT_ID", "")
tele_api_token = env.str("TELE_API_TOKEN", "")