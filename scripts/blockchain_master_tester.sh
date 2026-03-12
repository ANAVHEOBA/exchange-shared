#!/bin/bash
# scripts/blockchain_master_tester.sh

# This script acts as the master orchestrator for all blockchain health checks.
# It runs each parent chain's verification script and provides a summary.

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="scripts/rpc"
TOTAL=0
SUCCESS=0
FAILED=0
LOG_FILE="/tmp/blockchain_health_$(date +%Y%m%d_%H%M%S).log"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   🌟 BLOCKCHAIN INFRASTRUCTURE MASTER TESTER 🌟   ${NC}"
echo -e "${BLUE}======================================================${NC}"
echo "Logging results to: $LOG_FILE"
echo ""

# Find all check scripts, excluding check_all.sh
SCRIPTS=$(ls $SCRIPT_DIR/check_*.sh | grep -v "check_all.sh")

for SCRIPT in $SCRIPTS; do
    TOTAL=$((TOTAL + 1))
    
    # Run the script and capture its output
    OUTPUT=$($SCRIPT 2>&1)
    EXIT_CODE=$?
    
    # Extract the network name for the summary
    NAME=$(basename $SCRIPT | sed 's/check_//' | sed 's/.sh//' | tr '[:lower:]' '[:upper:]')
    
    # Log the full output
    echo "--- $NAME ---" >> "$LOG_FILE"
    echo "$OUTPUT" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"

    if [ $EXIT_CODE -eq 0 ]; then
        SUCCESS=$((SUCCESS + 1))
        # Find the successful provider in the output
        PROVIDER=$(echo "$OUTPUT" | grep "✅" | head -n 1 | awk -F': ' '{print $1}' | xargs)
        echo -e "${GREEN}✅ [PASS]${NC} ${CYAN}$NAME${NC} (via $PROVIDER)"
    else
        FAILED=$((FAILED + 1))
        echo -e "${RED}❌ [FAIL]${NC} ${CYAN}$NAME${NC}"
    fi
done

echo ""
echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}                FINAL SUMMARY REPORT                ${NC}"
echo -e "${BLUE}======================================================${NC}"
echo -e "Total Parent Blockchains: $TOTAL"
echo -e "Operational:             ${GREEN}$SUCCESS${NC}"
echo -e "Failed:                  ${RED}$FAILED${NC}"
echo -e "Success Rate:            $(awk "BEGIN {pc=100*$SUCCESS/$TOTAL; printf \"%.2f\", pc}")%"
echo -e "${BLUE}======================================================${NC}"

# If any failed, show the log location
if [ $FAILED -gt 0 ]; then
    echo -e "${YELLOW}Please check $LOG_FILE for detailed failure reports.${NC}"
fi

exit $((FAILED > 0))
