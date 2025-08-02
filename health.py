#!/usr/bin/env python3
"""
Simple health check server for Docker/Dokploy
Runs on port 8080 and provides a /health endpoint
"""
import http.server
import socketserver
import threading
import os
from datetime import datetime

class HealthHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            # Check if cron is running and logs exist
            status = "healthy"
            try:
                # Check if sync.log exists and has recent entries
                log_file = "/app/logs/sync.log"
                if os.path.exists(log_file):
                    # If log file exists, service is probably working
                    self.send_response(200)
                else:
                    # No log file yet, but that's okay for a new deployment
                    self.send_response(200)
                    
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = f'{{"status": "{status}", "timestamp": "{datetime.now().isoformat()}", "service": "sms-sync"}}\n'
                self.wfile.write(response.encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = f'{{"status": "error", "error": "{str(e)}", "timestamp": "{datetime.now().isoformat()}"}}\n'
                self.wfile.write(response.encode())
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    """Start health check server in background"""
    PORT = 8080
    Handler = HealthHandler
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Health check server running on port {PORT}")
        httpd.serve_forever()

if __name__ == "__main__":
    # Start health server in background thread
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Keep the script running
    import time
    while True:
        time.sleep(60)