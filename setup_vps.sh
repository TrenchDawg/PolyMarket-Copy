#!/bin/bash
# Polymarket Order Proxy — VPS Setup Script
# Run this on the Vultr Madrid server after SSH-ing in as root

set -e

echo "=== Updating system ==="
apt update && apt upgrade -y

echo "=== Installing Python and pip ==="
apt install -y python3 python3-pip python3-venv

echo "=== Creating app directory ==="
mkdir -p /opt/order-proxy
cd /opt/order-proxy

echo "=== Creating virtual environment ==="
python3 -m venv venv
source venv/bin/activate

echo "=== Installing dependencies ==="
pip install flask py-clob-client gunicorn requests

echo "=== Creating environment file ==="
cat > /opt/order-proxy/.env << 'ENVEOF'
# FILL THESE IN — do not commit this file anywhere
PROXY_AUTH_TOKEN=GENERATE_ONE_WITH_python3_-c_"import secrets; print(secrets.token_hex(32))"
POLY_PRIVATE_KEY=your-private-key-here
POLY_API_KEY=your-api-key-here
POLY_API_SECRET=your-api-secret-here
POLY_API_PASSPHRASE=your-api-passphrase-here
ENVEOF

echo "=== Creating systemd service ==="
cat > /etc/systemd/system/order-proxy.service << 'SVCEOF'
[Unit]
Description=Polymarket Order Proxy
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/order-proxy
EnvironmentFile=/opt/order-proxy/.env
ExecStart=/opt/order-proxy/venv/bin/gunicorn --bind 0.0.0.0:8080 --workers 2 --timeout 30 order_proxy:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "1. Copy order_proxy.py to /opt/order-proxy/"
echo "2. Edit /opt/order-proxy/.env with your real credentials"
echo "3. Generate an auth token: python3 -c \"import secrets; print(secrets.token_hex(32))\""
echo "4. Start the service:"
echo "   systemctl daemon-reload"
echo "   systemctl enable order-proxy"
echo "   systemctl start order-proxy"
echo "5. Check status: systemctl status order-proxy"
echo "6. Check logs: journalctl -u order-proxy -f"
echo "7. Test: curl http://YOUR_IP:8080/health"
echo "8. Test geocheck: curl http://YOUR_IP:8080/geocheck"
