#!/bin/bash
set -e

echo "🚀 Starting deployment to EC2..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Update system
echo -e "${YELLOW}📦 Updating system packages...${NC}"
sudo apt update && sudo apt upgrade -y

# Install Rust if not installed
if ! command -v cargo &> /dev/null; then
    echo -e "${YELLOW}🦀 Installing Rust...${NC}"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
else
    echo -e "${GREEN}✅ Rust already installed${NC}"
fi

# Install dependencies
echo -e "${YELLOW}📚 Installing build dependencies...${NC}"
sudo apt install -y build-essential pkg-config libssl-dev git mysql-client

# Check if .env exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}⚠️  .env file not found. Please create it before continuing.${NC}"
    exit 1
fi

# Build the application
echo -e "${YELLOW}🔨 Building application (this may take 10-20 minutes)...${NC}"
cargo build --release

# Create systemd service
echo -e "${YELLOW}⚙️  Setting up systemd service...${NC}"
sudo tee /etc/systemd/system/exchange-backend.service > /dev/null <<EOF
[Unit]
Description=Exchange Backend Service
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$PWD
Environment="PATH=$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=$PWD/target/release/exchange-shared
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd and start service
sudo systemctl daemon-reload
sudo systemctl enable exchange-backend
sudo systemctl restart exchange-backend

echo -e "${GREEN}✅ Deployment complete!${NC}"
echo ""
echo "📊 Service status:"
sudo systemctl status exchange-backend --no-pager

echo ""
echo "📝 View logs with: sudo journalctl -u exchange-backend -f"
echo "🌐 API should be running on: http://$(curl -s ifconfig.me):3000"
echo "🏥 Health check: curl http://localhost:3000/health"
