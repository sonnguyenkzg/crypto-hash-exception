# n8n Quick Management Commands

## 🚀 **Essential Commands (Copy & Paste)**

### **Check n8n Status**
```bash
docker ps | grep n8n
```

### **Stop n8n**
```bash
docker stop n8n
```

### **Start n8n** (if stopped)
```bash
docker start n8n
```

### **Restart n8n** (stop + start)
```bash
docker restart n8n
```

### **Stop and Remove n8n** (complete reset)
```bash
docker stop n8n && docker rm n8n
```

### **Create Fresh n8n Container**
```bash
docker run -d \
  --name n8n \
  --restart unless-stopped \
  -p 5678:5678 \
  -v ~/.n8n:/home/node/.n8n \
  -e TZ=UTC \
  n8nio/n8n
```

---

## 📊 **Status Check Commands**

### **Quick Status**
```bash
# Container running?
docker ps | grep n8n && echo "✅ Running" || echo "❌ Stopped"

# Port accessible?
curl -s -o /dev/null -w "%{http_code}" http://localhost:5678
```

### **Detailed Status**
```bash
echo "=== n8n Status ==="
echo "Container: $(docker ps --format 'table {{.Names}}\t{{.Status}}' | grep n8n || echo 'Not running')"
echo "Port 5678: $(ss -tulpn | grep 5678 && echo 'Open' || echo 'Closed')"
echo "HTTP Response: $(curl -s -o /dev/null -w "%{http_code}" http://localhost:5678)"
```

---

## 🔧 **Troubleshooting Commands**

### **View n8n Logs**
```bash
# Recent logs
docker logs n8n

# Follow logs in real-time
docker logs -f n8n

# Last 50 lines
docker logs --tail 50 n8n
```

### **Fix Docker Permissions** (if needed)
```bash
newgrp docker
```

### **Check Resources**
```bash
# Container resource usage
docker stats n8n --no-stream

# System resources
echo "RAM: $(free -h | awk '/^Mem:/ {print $3 "/" $2}')"
echo "Disk: $(df -h / | awk 'NR==2 {print $3 "/" $2}')"
```

---

## ⚡ **One-Liner Commands**

### **Complete Restart**
```bash
docker restart n8n && echo "✅ n8n restarted"
```

### **Fresh Install**
```bash
docker stop n8n 2>/dev/null; docker rm n8n 2>/dev/null; docker run -d --name n8n --restart unless-stopped -p 5678:5678 -v ~/.n8n:/home/node/.n8n -e TZ=UTC n8nio/n8n && echo "✅ Fresh n8n container created"
```

### **Quick Health Check**
```bash
docker ps | grep n8n | grep -q Up && curl -s localhost:5678 >/dev/null && echo "✅ n8n healthy" || echo "❌ n8n problem"
```

---

## 🌐 **Access Commands**

### **Local Access** (always works)
```bash
echo "Local: http://localhost:5678"
```

### **External Access** (if Security Group allows)
```bash
echo "External: http://$(curl -s ifconfig.me):5678"
```

### **SSH Tunnel** (bypass Security Group)
```bash
# Run on your local machine:
ssh -L 8080:localhost:5678 ubuntu@52.77.239.244
# Then access: http://localhost:8080
```

---

## 📋 **Common Scenarios**

### **Scenario 1: VS Code Disconnected, Reconnecting**
```bash
# After reconnecting to VS Code:
newgrp docker          # Fix permissions
docker ps | grep n8n   # Check if still running
```

### **Scenario 2: n8n Not Responding**
```bash
docker logs n8n        # Check for errors
docker restart n8n     # Restart container
```

### **Scenario 3: Port 5678 Blocked**
```bash
# Check what's using the port
sudo ss -tulpn | grep 5678

# Stop conflicting service and restart n8n
docker restart n8n
```

### **Scenario 4: Update n8n**
```bash
docker stop n8n
docker rm n8n
docker pull n8nio/n8n
# Run the "Create Fresh n8n Container" command above
```

---

## 🎯 **Quick Copy Commands**

**Stop:** `docker stop n8n`

**Start:** `docker start n8n`

**Restart:** `docker restart n8n`

**Status:** `docker ps | grep n8n`

**Logs:** `docker logs n8n`

**Fresh Install:** `docker stop n8n 2>/dev/null; docker rm n8n 2>/dev/null; docker run -d --name n8n --restart unless-stopped -p 5678:5678 -v ~/.n8n:/home/node/.n8n -e TZ=UTC n8nio/n8n`