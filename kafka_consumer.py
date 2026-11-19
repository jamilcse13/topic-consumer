
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING


ENV_PATH = Path(__file__).with_name(".env")


def load_env_file(path=ENV_PATH):
    """Load simple KEY=VALUE entries without requiring python-dotenv."""
    if not path.exists():
        raise FileNotFoundError(f"Environment file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        for line_number, raw_line in enumerate(file, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                raise ValueError(f"Invalid .env entry on line {line_number}")
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise ValueError(f"Empty .env key on line {line_number}")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ.setdefault(key, value)


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in ("true", "1", "yes", "on"):
        return True
    if normalized in ("false", "0", "no", "off"):
        return False
    raise ValueError(f"{name} must be true or false")


def env_int(name, default):
    value = os.getenv(name)
    try:
        return default if value is None else int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def load_config():
    load_env_file()
    bootstrap_servers = [
        server.strip()
        for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(",")
        if server.strip()
    ]
    return {
        "bootstrap_servers": bootstrap_servers,
        "topic": os.getenv("KAFKA_TOPIC"),
        "group_id": os.getenv("KAFKA_GROUP_ID"),
        "mode": os.getenv("MODE", "count"),
        "sample_limit": env_int("SAMPLE_LIMIT", 10),
        "raw_sample_file": os.getenv(
            "RAW_SAMPLE_FILE", "raw_sample_messages.json"
        ),
        "mapped_sample_file": os.getenv(
            "MAPPED_SAMPLE_FILE", "mapped_sample_messages.json"
        ),
        "notification_time_field": os.getenv(
            "NOTIFICATION_TIME_FIELD", "notificationTime"
        ),
        "auto_offset_reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "start_from_beginning": env_bool("START_FROM_BEGINNING", False),
        "idle_timeout": env_int("IDLE_TIMEOUT", 10),
        "max_runtime": env_int("MAX_RUNTIME", 300),
        "security": {
            "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        },
    }


def format_message(value):
    if value is None:
        return "<NULL>"

    try:
        decoded = value.decode("utf-8")
        try:
            return json.dumps(
                json.loads(decoded),
                indent=2,
                ensure_ascii=False
            )
        except json.JSONDecodeError:
            return decoded
    except (UnicodeDecodeError, AttributeError):
        return repr(value)


def decode_message(value):
    """Decode a Kafka value, preserving non-JSON payloads as text."""
    if value is None:
        return None

    try:
        decoded = value.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        return repr(value)

    try:
        return json.loads(decoded)
    except json.JSONDecodeError:
        return decoded


def get_field(data, field_path):
    """Read a top-level or dot-separated field from a decoded message."""
    value = data
    for field in field_path.split("."):
        if not isinstance(value, dict) or field not in value:
            return None
        value = value[field]
    return value


def notification_date(value):
    """Convert a notification timestamp to a date in the local timezone."""
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        # Support Unix timestamps expressed in seconds or milliseconds.
        seconds = value / 1000 if abs(value) >= 100_000_000_000 else value
        return datetime.fromtimestamp(seconds).astimezone().date()

    text = str(value).strip()
    if not text:
        return None

    if text.replace(".", "", 1).isdigit():
        return notification_date(float(text))

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone()
    return parsed.date()


def _parse_db_timestamp(value):
    """Normalize a message timestamp for storage in a database-style field."""
    if value is None or value == "":
        return None

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000 if abs(value) >= 100_000_000_000 else value
        parsed = datetime.fromtimestamp(seconds).astimezone()
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.replace(".", "", 1).isdigit():
            return _parse_db_timestamp(float(text))
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)

    return parsed.isoformat(sep=" ")


def _map_product_fields(message_data):
    """Map a T71 product survey event to sms_survey_dump columns."""
    return (
        message_data.get("MSISDN"),  # msisdn
        message_data.get("id"),  # trx_id
        _parse_db_timestamp(message_data.get("incoming_time")),
        None,  # source - not provided by T71
        None,  # channel - not provided by T71
        None,  # file_name - not provided by T71
        "sms-survey-product",  # trigger_name for T71
        None,  # user_id - not provided by T71
        None,  # user_name - not provided by T71
        None,  # branch_name - not provided by T71
        None,  # wc - not provided by T71
        message_data.get("triggerDescription"),  # reasons
        None,  # outcome - not provided by T71
        message_data.get("TRIGGER_ATTR_04"),  # arpu_segment
        message_data.get("TRIGGER_ATTR_07"),  # region
        message_data.get("TRIGGER_ATTR_02", 0),  # is_data_user
        0,  # is_processed
        _parse_db_timestamp(message_data.get("notificationTime")),
    )


def main():
    config = load_config()

    required_fields = ("bootstrap_servers", "topic", "group_id")
    missing_fields = [field for field in required_fields if not config.get(field)]
    if missing_fields:
        raise ValueError(
            "Missing required config field(s): " + ", ".join(missing_fields)
        )

    if not isinstance(config["bootstrap_servers"], list):
        raise ValueError("'bootstrap_servers' must be a JSON list")

    kafka_config = {
        "bootstrap.servers": ",".join(
            config["bootstrap_servers"]
        ),
        "group.id": config["group_id"],
        "auto.offset.reset": config.get(
            "auto_offset_reset", "earliest"
        ),
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "session.timeout.ms": 30000
    }

    # Apply additional Kafka security settings
    kafka_config.update(config.get("security", {}))

    topic = config["topic"]
    mode = config.get("mode", "count").lower()
    sample_limit = config.get("sample_limit", 10)
    raw_sample_file_name = Path(
        config.get("raw_sample_file", "raw_sample_messages.json")
    ).name
    mapped_sample_file_name = Path(
        config.get("mapped_sample_file", "mapped_sample_messages.json")
    ).name
    run_datetime = datetime.now().astimezone()
    sample_directory = (
        Path(__file__).parent
        / "sample_files"
        / run_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    )
    raw_sample_file = sample_directory / raw_sample_file_name
    mapped_sample_file = sample_directory / mapped_sample_file_name
    notification_time_field = config.get(
        "notification_time_field", "notificationTime"
    )
    start_from_beginning = config.get("start_from_beginning", False)
    idle_timeout = config.get("idle_timeout", 10)
    max_runtime = config.get("max_runtime", 300)

    if mode not in ("count", "sample"):
        raise ValueError("Mode must be 'count' or 'sample'")
    if not isinstance(sample_limit, int) or sample_limit < 0:
        raise ValueError("'sample_limit' must be a non-negative integer")

    total_messages = 0
    today_count = 0
    yesterday_count = 0
    missing_notification_time = 0
    invalid_notification_time = 0
    raw_samples = []
    mapped_samples = []
    today = run_datetime.date()
    yesterday = today - timedelta(days=1)
    start_time = time.monotonic()
    last_message_time = start_time
    consumer = None

    print("=" * 60)
    print("KAFKA CONSUMER STARTED")
    print("=" * 60)
    print(f"Topic       : {topic}")
    print(f"Group ID    : {config['group_id']}")
    print(f"Mode        : {mode}")
    print(f"Started at  : {datetime.now()}")
    print("=" * 60)

    try:
        consumer = Consumer(kafka_config)

        def on_assign(assigned_consumer, partitions):
            if start_from_beginning:
                for partition in partitions:
                    partition.offset = OFFSET_BEGINNING
            assigned_consumer.assign(partitions)

        consumer.subscribe([topic], on_assign=on_assign)

        while True:
            current_time = time.monotonic()

            # Maximum execution time
            if current_time - start_time >= max_runtime:
                print("\nMaximum runtime reached.")
                break

            # Stop when no new messages arrive
            if current_time - last_message_time >= idle_timeout:
                print("\nNo new messages within idle timeout.")
                break

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            total_messages += 1
            last_message_time = time.monotonic()
            decoded_value = decode_message(msg.value())

            raw_samples.append(decoded_value)
            if isinstance(decoded_value, dict):
                try:
                    mapped_samples.append(_map_product_fields(decoded_value))
                except (ValueError, TypeError, OverflowError, OSError):
                    mapped_samples.append(None)
            else:
                mapped_samples.append(None)

            # Keep only the latest samples while preserving raw/mapped alignment.
            if len(raw_samples) > sample_limit:
                raw_samples.pop(0)
                mapped_samples.pop(0)

            timestamp = get_field(decoded_value, notification_time_field)
            if timestamp is None:
                missing_notification_time += 1
            else:
                try:
                    message_date = notification_date(timestamp)
                except (ValueError, TypeError, OverflowError, OSError):
                    invalid_notification_time += 1
                else:
                    if message_date == today:
                        today_count += 1
                    elif message_date == yesterday:
                        yesterday_count += 1

            if mode == "sample":
                print(f"\n--- Message #{total_messages} ---")
                print(f"Partition : {msg.partition()}")
                print(f"Offset    : {msg.offset()}")
                print(f"Key       : {format_message(msg.key())}")
                print(f"Value     : {format_message(msg.value())}")

    except KeyboardInterrupt:
        print("\nConsumer interrupted by user.")

    except Exception as e:
        print(f"\nKafka consumer error: {e}")
        raise

    finally:
        if consumer is not None:
            consumer.close()

        sample_directory.mkdir(parents=True, exist_ok=True)

        with raw_sample_file.open("w", encoding="utf-8") as file:
            json.dump(raw_samples, file, indent=2, ensure_ascii=False)
            file.write("\n")

        with mapped_sample_file.open("w", encoding="utf-8") as file:
            json.dump(mapped_samples, file, indent=2, ensure_ascii=False)
            file.write("\n")

        print("\n" + "=" * 60)
        print("KAFKA CONSUMPTION SUMMARY")
        print("=" * 60)
        print(f"Topic           : {topic}")
        print(f"Group ID        : {config['group_id']}")
        print(f"Total consumed  : {total_messages}")
        print(f"Today ({today})  : {today_count}")
        print(f"Yesterday ({yesterday}): {yesterday_count}")
        print(f"Missing {notification_time_field}: {missing_notification_time}")
        print(f"Invalid {notification_time_field}: {invalid_notification_time}")
        print(f"Raw samples     : {len(raw_samples)} -> {raw_sample_file}")
        print(f"Mapped samples  : {len(mapped_samples)} -> {mapped_sample_file}")
        print(f"Execution time  : {time.monotonic() - start_time:.2f}s")
        print("=" * 60)


if __name__ == "__main__":
    main()
