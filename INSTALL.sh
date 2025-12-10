#!/bin/bash
#
# Nodographer Installation Script
# -------------------------------
# Automates the complete installation process
#
# Usage: sudo ./INSTALL.sh
#

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "\n${GREEN}===================================================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}===================================================================${NC}\n"
}

print_step() {
    echo -e "${YELLOW}➜${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run as root (use sudo)"
   exit 1
fi

# Get the actual user who ran sudo
ACTUAL_USER="${SUDO_USER:-$USER}"
if [ "$ACTUAL_USER" = "root" ]; then
    print_error "Please run this script with sudo from a regular user account, not as root"
    exit 1
fi

print_header "Nodographer Installation Script"

# Configuration variables
INSTALL_DIR="/srv/meshmap"
DB_NAME="node_map"
DB_USER="mesh-map"
DB_PASS="password"
WEB_ROOT="/var/www/html"

# Check repository access FIRST before any setup
print_header "Verifying Repository Access"
print_step "Checking SSH authentication to GitHub as $ACTUAL_USER..."

CLONE_METHOD=""
# Test SSH access without sudo to preserve SSH agent context
if ssh -o BatchMode=yes -o ConnectTimeout=5 git@github.com &>/dev/null; then
    print_success "SSH authentication verified"
    CLONE_METHOD="ssh"
else
    print_step "SSH key not configured, checking if repository is public via HTTPS..."
    if env GIT_TERMINAL_PROMPT=0 git ls-remote https://github.com/AI7BQ/nodographer.git &>/dev/null; then
        print_success "Repository is public; HTTPS access available"
        CLONE_METHOD="https"
    else
        print_error "Repository is private and SSH key is not configured"
        echo ""
        echo "To set up SSH authentication:"
        echo "  1. Add your public key to GitHub: https://github.com/settings/keys"
        echo "  2. Test SSH: ssh -T git@github.com"
        echo "  3. Try installation again"
        echo ""
        exit 1
    fi
fi

echo ""
echo "Installation Directory: $INSTALL_DIR"
echo "Database Name: $DB_NAME"
echo "Database User: $DB_USER"
echo "Web Server Root: $WEB_ROOT"
echo ""
read -p "Continue with installation? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_error "Installation cancelled"
    exit 1
fi

# Step 1: Install required packages
print_header "Step 1: Installing Required Packages"
print_step "Updating package lists..."
apt-get update -qq

print_step "Installing python3, python3-venv, python3-pip, git, mariadb-server, nginx..."
apt-get install -y python3 python3-venv python3-pip git mariadb-server nginx > /dev/null 2>&1
print_success "Required packages installed"

# Step 2: Check if already installed
print_header "Step 2: Checking Existing Installation"
if [ -d "$INSTALL_DIR" ]; then
    print_error "Directory $INSTALL_DIR already exists!"
    read -p "Remove existing installation and continue? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Stopping service if running..."
        systemctl stop meshmapPoller.service 2>/dev/null || true
        systemctl disable meshmapPoller.service 2>/dev/null || true
        rm -f /etc/systemd/system/meshmapPoller.service
        systemctl daemon-reload
        
        print_step "Removing existing installation..."
        rm -rf "$INSTALL_DIR"
        rm -f "$WEB_ROOT/meshmap"
        print_success "Existing installation removed"
    else
        print_error "Installation cancelled"
        exit 1
    fi
fi

# Step 3: Clone repository
print_header "Step 3: Cloning Repository"
print_step "Creating $INSTALL_DIR directory..."
mkdir -p /srv
chown "$ACTUAL_USER":"$ACTUAL_USER" /srv
cd /srv

if [ "$CLONE_METHOD" = "ssh" ]; then
    print_step "Cloning from GitHub using SSH as $ACTUAL_USER..."
    sudo -u "$ACTUAL_USER" git clone -q git@github.com:AI7BQ/nodographer.git meshmap
else
    print_step "Cloning from GitHub using HTTPS as $ACTUAL_USER..."
    sudo -u "$ACTUAL_USER" env GIT_TERMINAL_PROMPT=0 git clone -q https://github.com/AI7BQ/nodographer.git meshmap
fi
print_success "Repository cloned to $INSTALL_DIR"

# Step 4: Create meshmap system user
print_header "Step 4: Creating System User"
if id "meshmap" &>/dev/null; then
    print_step "User 'meshmap' already exists, skipping..."
else
    print_step "Creating meshmap system user..."
    useradd -r -s /bin/false -d "$INSTALL_DIR" -c "MeshMap Poller Service" meshmap
    print_success "System user 'meshmap' created"
fi

# Step 5: Setup Python virtual environment
print_header "Step 5: Setting Up Python Virtual Environment"
print_step "Transferring backend ownership to meshmap user..."
chown -R meshmap:meshmap "$INSTALL_DIR/backend" "$INSTALL_DIR/.cache"

print_step "Creating Python virtual environment..."
cd "$INSTALL_DIR/backend"
sudo -H -u meshmap python3 -m venv venv

print_step "Installing Python dependencies..."
sudo -H -u meshmap venv/bin/pip install --upgrade pip -q
sudo -H -u meshmap venv/bin/pip install -r requirements.txt -q
print_success "Python virtual environment configured"

# Step 6: Database setup
print_header "Step 6: Setting Up Database"
print_step "Creating MariaDB database and user..."

# Check if database exists
DB_EXISTS=$(mysql -u root -sse "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='$DB_NAME'")
if [ "$DB_EXISTS" -eq 1 ]; then
    print_step "Database '$DB_NAME' already exists, skipping creation..."
else
    mysql -u root <<SQL
CREATE DATABASE IF NOT EXISTS $DB_NAME CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
SQL
    print_success "Database '$DB_NAME' created"
fi

# Check if user exists
USER_EXISTS=$(mysql -u root -sse "SELECT COUNT(*) FROM mysql.user WHERE user='$DB_USER' AND host='localhost'")
if [ "$USER_EXISTS" -eq 1 ]; then
    print_step "Database user '$DB_USER' already exists, updating privileges..."
else
    print_step "Creating database user '$DB_USER'..."
fi

mysql -u root <<SQL
CREATE USER IF NOT EXISTS '$DB_USER'@'localhost' IDENTIFIED BY '$DB_PASS';
GRANT ALL PRIVILEGES ON $DB_NAME.* TO '$DB_USER'@'localhost';
FLUSH PRIVILEGES;
SQL
print_success "Database configured (tables will be created automatically on first run)"

# Step 7: Configure application
print_header "Step 7: Configuring Application"
print_step "Configuration file is at: $INSTALL_DIR/settings.ini"
print_step "You may need to edit settings.ini to customize:"
echo "  - nodelistNode (default: localnode.local.mesh)"
echo "  - Map center coordinates"
echo "  - Distance units (miles/kilometers)"
echo "  - Database password (currently: $DB_PASS)"
print_success "Configuration ready (edit $INSTALL_DIR/settings.ini as needed)"

# Step 8: Setup web server
print_header "Step 8: Configuring Web Server"
print_step "Creating symlink to web root..."
ln -sf "$INSTALL_DIR/frontend" "$WEB_ROOT/meshmap"

print_step "Setting data directory permissions..."
chown -R meshmap:www-data "$INSTALL_DIR/frontend/data"
chmod 775 "$INSTALL_DIR/frontend/data"
print_success "Web server configured (http://<hostname>/meshmap/)"

# Step 9: Transfer ownership for production
print_header "Step 9: Setting Production Permissions"
print_step "Transferring ownership to meshmap user..."
chown -R meshmap:meshmap "$INSTALL_DIR/backend"
chown -R meshmap:www-data "$INSTALL_DIR/frontend/data"
chmod 775 "$INSTALL_DIR/frontend/data"
print_success "Ownership configured for production"

# Step 10: Install systemd service
print_header "Step 10: Installing systemd Service"
print_step "Creating systemd service symlink..."
ln -sf "$INSTALL_DIR/backend/meshmapPoller.service" /etc/systemd/system/meshmapPoller.service

print_step "Reloading systemd daemon..."
systemctl daemon-reload

print_step "Enabling and starting meshmapPoller service..."
systemctl enable meshmapPoller.service
systemctl start meshmapPoller.service
print_success "Service installed and started"

# Step 11: Verify installation
print_header "Step 11: Verifying Installation"
sleep 2  # Give service time to start

if systemctl is-active --quiet meshmapPoller.service; then
    print_success "Service is running"
else
    print_error "Service failed to start. Check logs with: journalctl -u meshmapPoller.service"
fi

# Final summary
print_header "Installation Complete!"
echo ""
echo "Next steps:"
echo "  1. Edit configuration if needed: sudo nano $INSTALL_DIR/settings.ini"
echo "  2. Restart service after config changes: sudo systemctl restart meshmapPoller.service"
echo "  3. View logs: sudo journalctl -fu meshmapPoller.service"
echo "  4. Access web interface: http://<hostname>/meshmap/"
echo ""
echo "Service management:"
echo "  Status:  sudo systemctl status meshmapPoller.service"
echo "  Stop:    sudo systemctl stop meshmapPoller.service"
echo "  Restart: sudo systemctl restart meshmapPoller.service"
echo "  Logs:    sudo journalctl -u meshmapPoller.service -f"
echo ""
print_success "Installation completed successfully!"
