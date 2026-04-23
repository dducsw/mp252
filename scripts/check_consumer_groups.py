#!/usr/bin/env python3
"""
Script to check Kafka consumer groups and verify their existence.
"""

import sys
from kafka import KafkaAdminClient
from kafka.errors import KafkaError


def check_consumer_groups(bootstrap_servers: str = "127.0.0.1:9092", target_group: str = None):
    """
    Check and list all Kafka consumer groups.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (default: localhost:9092)
        target_group: Specific consumer group to check for (optional)
    """
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="check-consumer-groups"
        )
        
        # List all consumer groups
        future = admin_client.list_consumer_groups()
        consumer_groups_result = future.result()
        
        # Extract valid consumer groups (filter out errors)
        consumer_groups = []
        for group_info in consumer_groups_result.groups:
            consumer_groups.append(group_info.id)
        
        print("=" * 60)
        print("Kafka Consumer Groups:")
        print("=" * 60)
        
        if not consumer_groups:
            print("❌ No consumer groups found!")
        else:
            for i, group in enumerate(consumer_groups, 1):
                print(f"{i}. {group}")
        
        print("=" * 60)
        
        # Check for specific consumer group if provided
        if target_group:
            if target_group in consumer_groups:
                print(f"✅ Consumer group '{target_group}' EXISTS")
            else:
                print(f"❌ Consumer group '{target_group}' NOT FOUND")
                return False
        
        admin_client.close()
        return True
        
    except KafkaError as e:
        print(f"❌ Kafka Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Main entry point."""
    bootstrap_servers = "127.0.0.1:9092"
    target_group = "buswaypoint_consumer_group"
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        target_group = sys.argv[1]
    
    print(f"\nChecking Kafka at: {bootstrap_servers}")
    print(f"Target consumer group: {target_group}\n")
    
    success = check_consumer_groups(bootstrap_servers, target_group)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
