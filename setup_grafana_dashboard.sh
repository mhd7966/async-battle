#!/bin/bash

echo "Setting up Grafana Dashboard..."

# Wait for Grafana to be ready
echo "Waiting for Grafana to be ready..."
# sleep 30

# Check if Grafana is accessible
echo "Checking Grafana accessibility..."
until curl -s http://localhost:3000/api/health > /dev/null; do
    echo "Waiting for Grafana to start..."
    sleep 5
done
echo "✅ Grafana is accessible"

# Add Prometheus data source
echo "Adding Prometheus data source..."
curl -X POST http://localhost:3000/api/datasources \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{
    "name": "Prometheus",
    "type": "prometheus",
    "url": "http://prometheus:9090",
    "access": "proxy",
    "isDefault": true
  }' 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Prometheus data source added"
else
    echo "ℹ️  Prometheus data source might already exist"
fi

# Import the dashboard
echo "Importing RabbitMQ dashboard..."
curl -X POST http://localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d @grafana_dashboard.json 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Dashboard imported successfully"
else
    echo "❌ Failed to import dashboard"
fi

echo ""
echo "🎉 Grafana setup completed!"
echo "📊 Access Grafana at: http://localhost:3000 (admin/admin)"
echo "📈 The RabbitMQ dashboard should now be available in the dashboards list"
echo ""
echo "To manually import the dashboard:"
echo "1. Go to http://localhost:3000"
echo "2. Login with admin/admin"
echo "3. Click the '+' icon in the sidebar"
echo "4. Select 'Import'"
echo "5. Click 'Upload JSON file'"
echo "6. Select the grafana_dashboard.json file"
echo "7. Click 'Import'" 