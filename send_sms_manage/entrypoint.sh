# Create the entrypoint.sh file

#!/bin/bash
# Check if log directory is writable

if [ -w "/app/logs" ]; then
    echo "Log directory is writable"
else
    echo "Warning: Log directory may not be writable, but continuing..."
fi

# Execute the command passed to the entrypoint
exec "$@"
EOF

# Make it executable
chmod +x ./send_sms_manage/entrypoint.sh