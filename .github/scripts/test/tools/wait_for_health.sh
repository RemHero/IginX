#!/bin/bash

container_name="$1"

if [ -z "$container_name" ]; then
    echo "Usage: $0 <container_name>"
    exit 1
fi

while true; do
    health_status=$(docker inspect --format='{{json .State.Health.Status}}' "$container_name")

    if [[ $health_status == '"healthy"' ]]; then
        echo "Container $container_name is now healthy."
        break
    else
        echo "Container $container_name is not healthy yet, is $health_status. Waiting..."
        sleep 5
    fi
done
