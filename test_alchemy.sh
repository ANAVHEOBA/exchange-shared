#!/bin/bash

# Test script to verify Alchemy RPC integration
# This script tests the RPC configuration and Alchemy API key setup

echo "🧪 Testing Alchemy RPC Integration"
echo "=================================="
echo ""

# Check if ALCHEMY_API_KEY is set in .env
if grep -q "^ALCHEMY_API_KEY=.\+" .env; then
    echo "✅ ALCHEMY_API_KEY found in .env file"
    API_KEY=$(grep "^ALCHEMY_API_KEY=" .env | cut -d'=' -f2)
    if [ "$API_KEY" != "" ] && [ "$API_KEY" != "YOUR_ALCHEMY_API_KEY" ]; then
        echo "🔑 API Key: ${API_KEY:0:8}...${API_KEY: -4}"
    else
        echo "⚠️  ALCHEMY_API_KEY is empty or placeholder"
        echo "💡 To use Alchemy:"
        echo "   1. Sign up at https://www.alchemy.com"
        echo "   2. Create a new app"
        echo "   3. Copy your API key"
        echo "   4. Update ALCHEMY_API_KEY in .env file"
        echo ""
        echo "📝 For now, using public endpoints (slower, rate limited)"
    fi
else
    echo "⚠️  ALCHEMY_API_KEY not found in .env file"
    echo "📝 Using public endpoints (slower, rate limited)"
fi

echo ""
echo "🧪 Running RPC configuration tests..."
echo ""

# Run the Rust tests
cargo test --test rpc_alchemy_test -- --nocapture

echo ""
echo "=================================="
echo "✅ Test complete!"
echo ""
echo "💡 Next steps:"
echo "   1. If tests passed with public endpoints, you're good to go!"
echo "   2. To use Alchemy (recommended), add your API key to .env"
echo "   3. Run this script again to verify Alchemy integration"
