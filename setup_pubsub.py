#!/usr/bin/env python3
"""
Comprehensive Pub/Sub Setup Script
Sets up Pub/Sub topics, subscriptions, and infrastructure across all projects
"""

import os
import sys
import subprocess
from typing import List, Dict

# Load environment variables from .env file
def load_env():
    """Load environment variables from .env file"""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

# Load environment variables at startup
load_env()

class PubSubSetup:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.topics_to_create = [
            "realtime-events",
            "data-quality-alerts", 
            "pipeline-notifications",
            "automation-events",
            "migration-status"
        ]
        
        self.subscriptions_to_create = [
            ("realtime-events", "realtime-events-sub"),
            ("data-quality-alerts", "quality-alerts-sub"),
            ("pipeline-notifications", "pipeline-notifications-sub"),
            ("automation-events", "automation-events-sub"),
            ("migration-status", "migration-status-sub")
        ]
    
    def run_command(self, command: str) -> bool:
        """Run a shell command and return success status"""
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                print(f"✅ Success: {command}")
                return True
            else:
                print(f"❌ Failed: {command}")
                print(f"   Error: {result.stderr}")
                return False
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout: {command}")
            return False
        except Exception as e:
            print(f"❌ Exception: {command} - {e}")
            return False
    
    def enable_pubsub_api(self) -> bool:
        """Enable Pub/Sub API"""
        command = f"gcloud services enable pubsub.googleapis.com --project={self.project_id}"
        return self.run_command(command)
    
    def create_topics(self) -> bool:
        """Create all Pub/Sub topics"""
        print(f"\nCreating Pub/Sub topics...")
        success = True
        
        for topic_name in self.topics_to_create:
            command = f"gcloud pubsub topics create {topic_name} --project={self.project_id}"
            if not self.run_command(command):
                success = False
        
        return success
    
    def create_subscriptions(self) -> bool:
        """Create all Pub/Sub subscriptions"""
        print(f"\nCreating Pub/Sub subscriptions...")
        success = True
        
        for topic_name, sub_name in self.subscriptions_to_create:
            command = f"gcloud pubsub subscriptions create {sub_name} --topic={topic_name} --project={self.project_id}"
            if not self.run_command(command):
                success = False
        
        return success
    
    def verify_setup(self) -> bool:
        """Verify that all topics and subscriptions were created"""
        print(f"\nVerifying Pub/Sub setup...")
        
        # List topics
        command = f"gcloud pubsub topics list --project={self.project_id} --format='value(name)'"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            created_topics = result.stdout.strip().split('\n')
            print(f"   Topics found: {len(created_topics)}")
            for topic in created_topics:
                if topic:
                    print(f"   - {topic.split('/')[-1]}")
        else:
            print(f"   ERROR: Could not list topics")
            return False
        
        # List subscriptions
        command = f"gcloud pubsub subscriptions list --project={self.project_id} --format='value(name)'"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            created_subs = result.stdout.strip().split('\n')
            print(f"   Subscriptions found: {len(created_subs)}")
            for sub in created_subs:
                if sub:
                    print(f"   - {sub.split('/')[-1]}")
        else:
            print(f"   ERROR: Could not list subscriptions")
            return False
        
        return True
    
    def setup_all(self) -> bool:
        """Run complete Pub/Sub setup"""
                
        steps = [
            ("Enable Pub/Sub API", self.enable_pubsub_api),
            ("Create Topics", self.create_topics),
            ("Create Subscriptions", self.create_subscriptions),
            ("Verify Setup", self.verify_setup)
        ]
        
        for step_name, step_func in steps:
            print(f"\n{step_name}...")
            if not step_func():
                print(f"ERROR: Failed at {step_name}")
                return False
        
        print(f"\nPub/Sub setup completed successfully!")
        return True
    
    def cleanup_resources(self) -> bool:
        """Clean up all Pub/Sub resources"""
        print(f"Cleaning up Pub/Sub resources...")
        
        # Delete subscriptions first
        for topic_name, sub_name in self.subscriptions_to_create:
            command = f"gcloud pubsub subscriptions delete {sub_name} --project={self.project_id} --quiet"
            self.run_command(command)
        
        # Delete topics
        for topic_name in self.topics_to_create:
            command = f"gcloud pubsub topics delete {topic_name} --project={self.project_id} --quiet"
            self.run_command(command)
        
        return True


def main():
    """Main function"""
    # Get project ID from environment or prompt user
    project_id = os.getenv('GCP_PROJECT_ID')
    if not project_id:
        print("ERROR: GCP_PROJECT_ID environment variable not set!")
        print("Please set it with: export GCP_PROJECT_ID=your-project-id")
        return 1
    
    # Check if cleanup is requested
    if len(sys.argv) > 1 and sys.argv[1] == '--cleanup':
        setup = PubSubSetup(project_id)
        setup.cleanup_resources()
        return 0
    
    # Run setup
    setup = PubSubSetup(project_id)
    if setup.setup_all():
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit(main())
