#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mesh Map Polling Daemon - Python Implementation

This script polls AREDN mesh nodes, collects network topology,
and stores the data for mapping visualization.

Original PHP implementation: KG6WXC 2016-2024
Python daemon implementation: 2025

Licensed under GPLv3 or later
"""

import asyncio
import aiohttp
import signal
import sys
import os
import argparse
import logging
from logging.handlers import SysLogHandler
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import json
import pickle
import configparser
import math
import re
from dataclasses import dataclass, field, asdict
from decimal import Decimal
import time


# ----------------------------------------------------------------------------
# Firmware classification helpers (mirrors filter.py logic)
# ----------------------------------------------------------------------------

def version_to_int(version: str) -> Optional[int]:
    """Convert dotted version string to sortable integer (e.g., 3.25.5.0)."""
    if not version or not isinstance(version, str):
        return None
    parts = version.split('.')
    try:
        parts = [int(p) for p in parts]
    except ValueError:
        return None
    parts = parts + [0] * (4 - len(parts))
    return parts[0] * 1_000_000 + parts[1] * 10_000 + parts[2] * 100 + parts[3]


def nightly_to_int(nightly: str) -> Optional[int]:
    """Convert nightly build identifier (YYYYMMDD-hash) to sortable integer."""
    if not nightly or not isinstance(nightly, str):
        return None
    parts = nightly.split('-')
    try:
        return int(parts[0])
    except ValueError:
        return None


def _parse_last_seen(last_seen: Any) -> Optional[float]:
    """Normalize last_seen to a unix timestamp."""
    if isinstance(last_seen, datetime):
        return last_seen.timestamp()
    if isinstance(last_seen, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(last_seen, fmt).timestamp()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(last_seen).timestamp()
        except Exception:
            return None
    return None


def _to_iso8601_utc(timestamp_value: Any) -> str:
    """Convert a timestamp (returned in the server's local time) to ISO 8601 UTC.

    MariaDB TIMESTAMP columns are stored as UTC but are returned to the session
    in the session's timezone (commonly the host's local time). We normalize by:
      1) parsing the local-time string/datetime
      2) converting to a Unix timestamp (interpreted as local time)
      3) emitting an explicit UTC ISO 8601 string with trailing Z
    Returns empty string if conversion fails.
    """

    if not timestamp_value:
        return ''

    try:
        # If it's already a datetime object (naive assumed local, aware handled)
        if isinstance(timestamp_value, datetime):
            if timestamp_value.tzinfo:
                utc_dt = timestamp_value.astimezone(timezone.utc)
            else:
                ts = timestamp_value.timestamp()  # treat naive as local time
                utc_dt = datetime.utcfromtimestamp(ts)
            return utc_dt.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')

        # If it's a string, parse it and convert to UTC
        if isinstance(timestamp_value, str):
            # Handle ISO strings with Z or offsets first
            iso_val = timestamp_value
            if iso_val.endswith('Z'):
                iso_val = iso_val[:-1] + '+00:00'
            if 'T' in iso_val and ('+' in iso_val[iso_val.find('T'):] or '-' in iso_val[iso_val.find('T'):]):
                try:
                    dt = datetime.fromisoformat(iso_val)
                    utc_dt = dt.astimezone(timezone.utc)
                    return utc_dt.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    pass

            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
                try:
                    dt = datetime.strptime(timestamp_value, fmt)
                    ts = dt.timestamp()  # interpret naive as local time
                    utc_dt = datetime.utcfromtimestamp(ts)
                    return utc_dt.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    continue
            return ''
    except Exception:
        return ''

    return ''


def _is_firmware(version: str, fw_type: str, version_cutoff: int, nightly_cutoff: int) -> bool:
    """Classify firmware by version string using cutoff rules from filter.py."""
    if not version or not isinstance(version, str):
        return False

    version = version.strip()

    if fw_type == 'babel':
        return version.startswith('babel-')

    if fw_type == 'olsr':
        if re.fullmatch(r'^\d{1,2}\.\d{1,2}\.\d{1,2}\.\d{1,2}$', version):
            v_int = version_to_int(version)
            return v_int is not None and v_int < version_cutoff
        if re.fullmatch(r'^\d{8}-[0-9a-fA-F]{7,8}$', version):
            n_int = nightly_to_int(version)
            return n_int is not None and n_int < nightly_cutoff
        return False

    if fw_type == 'combo':
        # Reject babel-only versions
        if version.startswith('babel-'):
            return False
        if re.fullmatch(r'^\d{1,2}\.\d{1,2}\.\d{1,2}\.\d{1,2}$', version):
            v_int = version_to_int(version)
            return v_int is not None and v_int >= version_cutoff
        if re.fullmatch(r'^\d{8}-[0-9a-fA-F]{7,8}$', version):
            n_int = nightly_to_int(version)
            return n_int is not None and n_int >= nightly_cutoff
        return False

    return False

# ============================================================================
# Configuration Management
# ============================================================================

@dataclass
class NodeInfo:
    """Data class for node information"""
    node: str = ""
    wlan_ip: str = ""
    uptime: str = "Not Available"
    loadavg: str = "Not Available"
    model: str = "Not Available"
    firmware_version: str = "Not Available"
    ssid: str = "None"
    channel: str = "None"
    chanbw: str = "None"
    tunnel_installed: str = "false"
    active_tunnel_count: str = "0"
    lat: float = 0.0
    lon: float = 0.0
    wifi_mac_address: str = ""
    api_version: str = "0.0.0"
    board_id: str = "Not Available"
    firmware_mfg: str = "Not Available"
    grid_square: str = "Not Available"
    lan_ip: str = "Not Available"
    services: str = "Not Available"
    description: str = ""
    mesh_supernode: str = "false"
    mesh_gateway: str = "false"
    freq: str = "None"
    link_info: str = ""
    hopsAway: int = 0
    meshRF: str = "on"
    band: str = "Unknown"
    last_seen: Optional[datetime] = None
    response_time_ms: float = 0.0
    
    # Antenna info
    antGain: float = 0.0
    antBeam: float = 0.0
    antDesc: str = "Not Available"
    antBuiltin: str = "false"


class ConfigManager:
    """Manages configuration loading and validation"""
    
    def __init__(self, config_path: str = "../settings.ini"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    def _load_config(self) -> configparser.ConfigParser:
        """Load configuration from settings file"""
        config = configparser.ConfigParser()
        
        # Load user settings
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"\nConfiguration file not found: {self.config_path}\n"
            )
        config.read(self.config_path)
        
        return config
    
    def _strip_quotes(self, value: str) -> str:
        """Remove surrounding quotes from config values"""
        if isinstance(value, str):
            value = value.strip()
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                return value[1:-1]
        return value
    
    def get(self, section: str, key: str, fallback: Any = None) -> Any:
        """Get configuration value with fallback"""
        try:
            value = self.config.get(section, key)
            value = self._strip_quotes(value)
            # Convert boolean strings
            if isinstance(value, str) and value.lower() in ('true', 'false'):
                return value.lower() == 'true'
            return value
        except (configparser.NoSectionError, configparser.NoOptionError):
            return fallback
    
    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        """Get integer configuration value"""
        try:
            value = self.config.get(section, key)
            value = self._strip_quotes(value)
            return int(value)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return fallback
    
    def getfloat(self, section: str, key: str, fallback: float = 0.0) -> float:
        """Get float configuration value"""
        try:
            value = self.config.get(section, key)
            value = self._strip_quotes(value)
            return float(value)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return fallback


class MySQLAdapter:
    """MariaDB/MySQL database adapter using aiomysql"""

    def __init__(self, config: ConfigManager):
        self.config = config
        self.pool = None

    async def connect(self):
        """Create connection pool"""
        try:
            import aiomysql
            self.pool = await aiomysql.create_pool(
                host=self.config.get('user-settings', 'sql_server', 'localhost'),
                user=self.config.get('user-settings', 'sql_user', 'mesh-map'),
                password=self.config.get('user-settings', 'sql_passwd', ''),
                db=self.config.get('user-settings', 'sql_db', 'node_map'),
                autocommit=True,
                minsize=5,
                maxsize=20
            )
            logging.info("Connected to MariaDB")
            # Ensure tables exist
            logging.info("Checking/creating database tables...")
            await self._ensure_tables()
            logging.info("Database tables ready")
        except ImportError:
            logging.error("aiomysql not installed. Install with: pip install aiomysql")
            raise
        except Exception as e:
            logging.error(f"Failed to connect to MariaDB or create tables: {e}")
            raise

    async def _ensure_tables(self):
        """Create database tables if they don't exist"""
        sql_db_tbl_node = self.config.get('user-settings', 'sql_db_tbl_node', 'node_info')
        sql_db_tbl_map = self.config.get('user-settings', 'sql_db_tbl_map', 'map_info')
        sql_db_tbl_aredn = self.config.get('user-settings', 'sql_db_tbl_aredn', 'aredn_info')
        
        async with self.pool.acquire() as conn:
            # Suppress warnings for "table already exists"
            async with conn.cursor() as cur_check:
                await cur_check.execute("SET SESSION sql_notes = 0")
            
            async with conn.cursor() as cur:
                # Create node_info table if not exists
                await cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{sql_db_tbl_node}` (
                        `wlan_ip` VARCHAR(45) PRIMARY KEY,
                        `node` VARCHAR(255) DEFAULT NULL,
                        `uptime` VARCHAR(255) DEFAULT NULL,
                        `loadavg` VARCHAR(255) DEFAULT NULL,
                        `model` VARCHAR(255) DEFAULT NULL,
                        `firmware_version` VARCHAR(50) DEFAULT NULL,
                        `ssid` VARCHAR(255) DEFAULT NULL,
                        `channel` VARCHAR(50) DEFAULT NULL,
                        `chanbw` VARCHAR(50) DEFAULT NULL,
                        `tunnel_installed` VARCHAR(10) DEFAULT 'false',
                        `active_tunnel_count` VARCHAR(10) DEFAULT '0',
                        `lat` DECIMAL(12,7) DEFAULT 0.0,
                        `lon` DECIMAL(13,7) DEFAULT 0.0,
                        `wifi_mac_address` VARCHAR(17) DEFAULT NULL,
                        `api_version` VARCHAR(50) DEFAULT NULL,
                        `board_id` VARCHAR(50) DEFAULT NULL,
                        `firmware_mfg` VARCHAR(100) DEFAULT NULL,
                        `grid_square` VARCHAR(50) DEFAULT NULL,
                        `lan_ip` VARCHAR(45) DEFAULT NULL,
                        `services` TEXT DEFAULT NULL,
                        `description` TEXT DEFAULT NULL,
                        `mesh_supernode` VARCHAR(10) DEFAULT 'false',
                        `mesh_gateway` VARCHAR(10) DEFAULT 'false',
                        `freq` VARCHAR(50) DEFAULT NULL,
                        `link_info` MEDIUMTEXT DEFAULT NULL,
                        `hopsAway` INT DEFAULT 0,
                        `meshRF` VARCHAR(10) DEFAULT 'on',
                        `last_seen` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        `antGain` DECIMAL(5,2) DEFAULT 0,
                        `antBeam` DECIMAL(5,2) DEFAULT 0,
                        `antDesc` VARCHAR(255) DEFAULT NULL,
                        `antBuiltin` VARCHAR(10) DEFAULT 'false',
                        `response_time_ms` FLOAT DEFAULT 0.0,
                        INDEX `idx_node` (`node`),
                        INDEX `idx_hops` (`hopsAway`),
                        INDEX `idx_last_seen` (`last_seen`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                # Create map_info table if not exists
                await cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{sql_db_tbl_map}` (
                        `id` VARCHAR(50) PRIMARY KEY,
                        `numParallelThreads` INT DEFAULT 0,
                        `nodeTotal` INT DEFAULT 0,
                        `garbageReturned` INT DEFAULT 0,
                        `highestHops` INT DEFAULT 0,
                        `totalPolled` INT DEFAULT 0,
                        `noLocation` INT DEFAULT 0,
                        `mappableNodes` INT DEFAULT 0,
                        `mappableLinks` INT DEFAULT 0,
                        `pollingTimeSec` DECIMAL(10,2) DEFAULT 0,
                        `lastPollingRun` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                # Create aredn_info table if not exists
                await cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{sql_db_tbl_aredn}` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `version_type` VARCHAR(50) DEFAULT NULL,
                        `version` VARCHAR(50) DEFAULT NULL,
                        `updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
            # Re-enable warnings
            async with conn.cursor() as cur_check:
                await cur_check.execute("SET SESSION sql_notes = 1")
                
            logging.info("Database tables verified/created successfully")

    async def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logging.info("MariaDB connection closed")

    async def upsert_node(self, node_data: NodeInfo):
        """Insert or update node in database"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                services = node_data.services if node_data.services else ""
                link_info_insert = node_data.link_info if node_data.link_info else None
                link_info_update = node_data.link_info if node_data.link_info else None
                loadavg = node_data.loadavg if node_data.loadavg else ""

                # Round lat/lon to 7 decimal places to avoid truncation warnings
                if node_data.lat is not None:
                    node_data.lat = round(node_data.lat, 7)
                if node_data.lon is not None:
                    node_data.lon = round(node_data.lon, 7)

                # Validate and warn about lat/lon values
                if node_data.lat is not None and (node_data.lat < -90 or node_data.lat > 90):
                    logging.warning(f"Invalid lat value for node {node_data.node}: {node_data.lat} (should be -90 to 90). Data: node={node_data.node}, lat={node_data.lat}, lon={node_data.lon}")
                if node_data.lon is not None and (node_data.lon < -180 or node_data.lon > 180):
                    logging.warning(f"Invalid lon value for node {node_data.node}: {node_data.lon} (should be -180 to 180). Data: node={node_data.node}, lat={node_data.lat}, lon={node_data.lon}")

                sql = """
                    INSERT INTO node_info (
                        node, wlan_ip, uptime, loadavg, model, firmware_version,
                        ssid, channel, chanbw, tunnel_installed, active_tunnel_count,
                        lat, lon, wifi_mac_address, api_version, board_id,
                        firmware_mfg, grid_square, lan_ip, services, description,
                        mesh_supernode, mesh_gateway, freq, link_info, hopsAway,
                        meshRF, last_seen, antGain, antBeam, antDesc, antBuiltin, response_time_ms
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(),
                        %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        node=%s, uptime=%s, loadavg=%s, model=%s, firmware_version=%s,
                        ssid=%s, channel=%s, chanbw=%s, tunnel_installed=%s,
                        active_tunnel_count=%s, lat=%s, lon=%s, wifi_mac_address=%s,
                        api_version=%s, board_id=%s, firmware_mfg=%s, grid_square=%s,
                        lan_ip=%s, services=%s, description=%s, mesh_supernode=%s,
                        mesh_gateway=%s, freq=%s, link_info=COALESCE(%s, link_info), hopsAway=%s,
                        meshRF=%s, last_seen=NOW(), antGain=%s, antBeam=%s,
                        antDesc=%s, antBuiltin=%s, response_time_ms=%s
                """

                values = (
                    node_data.node, node_data.wlan_ip, node_data.uptime, loadavg,
                    node_data.model, node_data.firmware_version, node_data.ssid,
                    node_data.channel, node_data.chanbw, node_data.tunnel_installed,
                    node_data.active_tunnel_count, node_data.lat, node_data.lon,
                    node_data.wifi_mac_address, node_data.api_version,
                    node_data.board_id, node_data.firmware_mfg, node_data.grid_square,
                    node_data.lan_ip, services, node_data.description,
                    node_data.mesh_supernode, node_data.mesh_gateway, node_data.freq,
                    link_info_insert, node_data.hopsAway, node_data.meshRF,
                    node_data.antGain, node_data.antBeam, node_data.antDesc,
                    node_data.antBuiltin, node_data.response_time_ms,
                    node_data.node, node_data.uptime, loadavg, node_data.model,
                    node_data.firmware_version, node_data.ssid, node_data.channel,
                    node_data.chanbw, node_data.tunnel_installed,
                    node_data.active_tunnel_count, node_data.lat, node_data.lon,
                    node_data.wifi_mac_address, node_data.api_version,
                    node_data.board_id, node_data.firmware_mfg, node_data.grid_square,
                    node_data.lan_ip, services, node_data.description,
                    node_data.mesh_supernode, node_data.mesh_gateway, node_data.freq,
                    link_info_update, node_data.hopsAway, node_data.meshRF,
                    node_data.antGain, node_data.antBeam, node_data.antDesc,
                    node_data.antBuiltin, node_data.response_time_ms
                )

                try:
                    await cur.execute(sql, values)
                except Exception as e:
                    logging.error(f"Failed to upsert node {node_data.node}: {e}")
                    logging.error(f"Node data: node={node_data.node}, lat={node_data.lat}, lon={node_data.lon}, wlan_ip={node_data.wlan_ip}")
                    raise

    async def update_link_info(self, wlan_ip: str, link_data: Dict):
        """Update link information for a node"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                link_str = pickle.dumps(link_data).hex()
                sql = "UPDATE node_info SET link_info = %s WHERE wlan_ip = %s"
                await cur.execute(sql, (link_str, wlan_ip))

    async def get_all_nodes(self) -> List[Dict]:
        """Retrieve all nodes"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM node_info")
                columns = [desc[0] for desc in cur.description]
                rows = await cur.fetchall()
                return [dict(zip(columns, row)) for row in rows] if rows else []

    async def mark_node_inactive(self, wlan_ip: str):
        """Clear link info for inactive node"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                empty_link = pickle.dumps({}).hex()
                sql = "UPDATE node_info SET link_info = %s WHERE wlan_ip = %s"
                await cur.execute(sql, (empty_link, wlan_ip))

    async def save_polling_stats(self, stats: Dict):
        """Save polling statistics to map_info table"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = """
                    INSERT INTO map_info (
                        id, numParallelThreads, nodeTotal, garbageReturned,
                        highestHops, totalPolled, noLocation, mappableNodes,
                        mappableLinks, pollingTimeSec, lastPollingRun
                    ) VALUES (
                        'POLLINFO', %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    ) ON DUPLICATE KEY UPDATE
                        numParallelThreads=%s, nodeTotal=%s, garbageReturned=%s,
                        highestHops=%s, totalPolled=%s, noLocation=%s,
                        mappableNodes=%s, mappableLinks=%s, pollingTimeSec=%s,
                        lastPollingRun=NOW()
                """
                values = (
                    stats['numParallelThreads'], stats['nodeTotal'],
                    stats['garbageReturned'], stats['highestHops'],
                    stats['totalPolled'], stats['noLocation'],
                    stats['mappableNodes'], stats['mappableLinks'],
                    stats['pollingTimeSec'],
                    stats['numParallelThreads'], stats['nodeTotal'],
                    stats['garbageReturned'], stats['highestHops'],
                    stats['totalPolled'], stats['noLocation'],
                    stats['mappableNodes'], stats['mappableLinks'],
                    stats['pollingTimeSec']
                )
                await cur.execute(sql, values)

    async def flush_database(self):
        """Truncate the node_info table"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql = "TRUNCATE TABLE node_info"
                await cur.execute(sql)
                logging.info("Truncated node_info table")


# ============================================================================
# Network Polling
# ============================================================================

class NodePoller:
    """Handles individual node polling operations"""
    
    # Node timeout and retry settings
    NODE_TIMEOUT = 10  # seconds
    NODE_RETRY_DELAY = 5  # seconds
    MAX_RETRIES = 1
    
    # Band identification constants
    BAND_900_BOARD_IDS = ['0xe009', '0xe1b9', '0xe239']
    BAND_2GHZ_CHANNELS = ['-1', '-2', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11']
    BAND_3GHZ_CHANNELS = ['76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99']
    BAND_5GHZ_CHANNELS = ['37', '40', '44', '48', '52', '56', '60', '64', '100', '104', '108', '112', '116', '120', '124', '128', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '181', '182', '183', '184']
    
    def __init__(self, session: aiohttp.ClientSession, logger: logging.Logger):
        self.session = session
        self.logger = logger
        
    @staticmethod
    def check_band(channel: str, board_id: str = None) -> str:
        """Determine frequency band from channel and board_id"""
        if board_id and board_id in NodePoller.BAND_900_BOARD_IDS:
            return '900MHz'
        if channel in NodePoller.BAND_2GHZ_CHANNELS:
            return '2GHz'
        if channel in NodePoller.BAND_3GHZ_CHANNELS:
            return '3GHz'
        if channel in NodePoller.BAND_5GHZ_CHANNELS:
            return '5GHz'
        return 'Unknown'
    
    async def fetch_json(self, url: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
        """Fetch and parse JSON from URL with retries"""
        for attempt in range(retries + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=self.NODE_TIMEOUT)
                # allow_redirects=True is important because many nodes 301/302 to the actual endpoint
                async with self.session.get(url, timeout=timeout, allow_redirects=True) as response:
                    if response.status == 200:
                        text = await response.text()
                        # Remove non-printable characters
                        text = ''.join(char for char in text if char.isprintable() or char in '\n\r\t')
                        return json.loads(text)
                    else:
                        self.logger.debug(f"HTTP {response.status} from {url}")
                        return None
            except asyncio.TimeoutError:
                if attempt < retries:
                    self.logger.debug(f"Timeout fetching {url}, retry {attempt + 1}/{retries}")
                    await asyncio.sleep(self.NODE_RETRY_DELAY)
                else:
                    self.logger.debug(f"Final timeout on {url}")
                    return None
            except Exception as e:
                self.logger.debug(f"Error fetching {url}: {e}")
                return None
        return None
    
    async def poll_node(self, ip: str, hops: int = 0) -> Optional[NodeInfo]:
        """Poll a single node and return its information"""
        start_time = time.time()
        
        # Prefer modern /a/sysinfo, but fall back to legacy cgi-bin for compatibility
        sysinfo_candidates = [
            f"http://{ip}/a/sysinfo",
            f"http://{ip}:8080/a/sysinfo",
            f"http://{ip}/cgi-bin/sysinfo.json",
            f"http://{ip}:8080/cgi-bin/sysinfo.json",
        ]

        data = None
        for url in sysinfo_candidates:
            data = await self.fetch_json(url)
            if data:
                break

        if not data:
            self.logger.debug(f"Failed to poll node {ip}")
            return None
        
        try:
            node_info = self._parse_sysinfo(data, ip)
            node_info.hopsAway = hops
            node_info.band = self.check_band(node_info.channel, node_info.board_id)
            node_info.response_time_ms = round((time.time() - start_time) * 1000, 2)
            
            # Fetch link_info separately for each node (modern first, legacy fallback)
            link_candidates = [
                f"http://{ip}/a/sysinfo?link_info=1",
                f"http://{ip}:8080/a/sysinfo?link_info=1",
                f"http://{ip}/cgi-bin/sysinfo.json?link_info=1",
                f"http://{ip}:8080/cgi-bin/sysinfo.json?link_info=1",
            ]
            link_info_data = None
            for url in link_candidates:
                link_info_data = await self.fetch_json(url)
                if link_info_data:
                    break

            if link_info_data and isinstance(link_info_data, dict):
                link_info_dict = link_info_data.get('link_info')
                if link_info_dict and isinstance(link_info_dict, dict):
                    node_info.link_info = pickle.dumps(link_info_dict).hex()
            
            # Fetch services separately for each node (modern first, legacy fallback), local services only
            services_candidates = [
                f"http://{ip}/a/sysinfo?services_local=1",
                f"http://{ip}:8080/a/sysinfo?services_local=1",
                f"http://{ip}/cgi-bin/sysinfo.json?services_local=1",
                f"http://{ip}:8080/cgi-bin/sysinfo.json?services_local=1",
            ]
            services_data = None
            for url in services_candidates:
                services_data = await self.fetch_json(url)
                if services_data:
                    break

            if services_data and isinstance(services_data, dict):
                services_list = services_data.get('services_local') or services_data.get('services')
                if services_list is None:
                    services_list = []
                if isinstance(services_list, list):
                    # Always store the list (even if empty) so UI shows "No Published Services" correctly
                    node_info.services = pickle.dumps(services_list).hex()
                    if len(services_list) > 0:
                        self.logger.debug(f"Found {len(services_list)} services for node {ip}")
            
            return node_info
        except Exception as e:
            self.logger.error(f"Error parsing data from {ip}: {e}")
            return None
    
    def _parse_sysinfo(self, data: Dict, ip: str) -> NodeInfo:
        """Parse sysinfo.json data into NodeInfo object"""
        node_info = NodeInfo()
        node_info.wlan_ip = ip
        
        # Parse root-level fields
        for key, value in data.items():
            if key == 'node':
                node_info.node = value
            elif key == 'lat':
                node_info.lat = float(value) if value else 0.0
            elif key == 'lon':
                node_info.lon = float(value) if value else 0.0
            elif key == 'api_version':
                node_info.api_version = value
            elif key == 'grid_square':
                node_info.grid_square = value
            elif key == 'model':
                node_info.model = value
            elif key == 'board_id':
                node_info.board_id = value
            elif key == 'firmware_version':
                node_info.firmware_version = value
            elif key == 'firmware_mfg':
                node_info.firmware_mfg = value
            elif key == 'uptime':
                node_info.uptime = str(value)
            elif key == 'description':
                node_info.description = value
            
            # Parse sysinfo (nested object with uptime and loads)
            elif key == 'sysinfo' and isinstance(value, dict):
                if 'uptime' in value:
                    node_info.uptime = str(value['uptime'])
                if 'loads' in value and isinstance(value['loads'], list):
                    node_info.loadavg = pickle.dumps(value['loads']).hex()
            
            # Parse node_details (older API format)
            elif key == 'node_details' and isinstance(value, dict):
                if 'model' in value:
                    node_info.model = value['model']
                if 'board_id' in value:
                    node_info.board_id = value['board_id']
                if 'firmware_version' in value:
                    node_info.firmware_version = value['firmware_version']
                if 'firmware_mfg' in value:
                    node_info.firmware_mfg = value['firmware_mfg']
                if 'description' in value:
                    node_info.description = value['description']
                if 'mesh_gateway' in value:
                    node_info.mesh_gateway = 'true' if value['mesh_gateway'] in [1, '1', True, 'true'] else 'false'
                if 'mesh_supernode' in value:
                    node_info.mesh_supernode = 'true' if value['mesh_supernode'] in [1, '1', True, 'true'] else 'false'
            
            # Parse nested structures
            elif key == 'meshrf' and isinstance(value, dict):
                node_info.meshRF = value.get('status', 'on')
                node_info.ssid = value.get('ssid', 'None')
                node_info.channel = str(value.get('channel', 'None'))
                node_info.chanbw = str(value.get('chanbw', 'None'))
                node_info.freq = str(value.get('freq', 'None'))
                
                # Parse antenna info
                if 'antenna' in value and isinstance(value['antenna'], dict):
                    ant = value['antenna']
                    node_info.antGain = float(ant.get('gain', 0))
                    node_info.antBeam = float(ant.get('beamwidth', 0))
                    node_info.antDesc = ant.get('description', 'Not Available')
                    node_info.antBuiltin = str(ant.get('builtin', 'false'))
            
            elif key == 'tunnels' and isinstance(value, dict):
                node_info.tunnel_installed = str(value.get('tunnel_installed', 'false'))
                node_info.active_tunnel_count = str(value.get('active_tunnel_count', '0'))
            
            elif key == 'interfaces' and isinstance(value, list):
                for iface in value:
                    if not isinstance(iface, dict):
                        continue
                    name = iface.get('name', '')
                    ip_addr = iface.get('ip', '')
                    
                    if name == 'wlan0' or name == 'wlan1':
                        if 'mac' in iface:
                            node_info.wifi_mac_address = iface['mac']
                        if ip_addr and ip_addr != 'none':
                            node_info.wlan_ip = ip_addr
                    elif name == 'br-lan' and ip_addr and ip_addr != 'none':
                        node_info.lan_ip = ip_addr
                    elif name in ['eth1.3975', 'eth0.3975', 'br-nomesh', 'br0']:
                        if ip_addr and ip_addr != 'none' and ip_addr.startswith('10.'):
                            node_info.wlan_ip = ip_addr
            
            elif key == 'services_local' and isinstance(value, list):
                node_info.services = pickle.dumps(value).hex()
            
            elif key == 'link_info' and isinstance(value, dict):
                node_info.link_info = pickle.dumps(value).hex()
            
            elif key == 'loads' and isinstance(value, list):
                node_info.loadavg = pickle.dumps(value).hex()
            
            # Gateway and supernode flags
            elif key == 'mesh_gateway':
                node_info.mesh_gateway = 'true' if value in [1, '1', True, 'true'] else 'false'
            elif key == 'mesh_supernode':
                node_info.mesh_supernode = 'true' if value in [1, '1', True, 'true'] else 'false'
        
        return node_info


# ============================================================================
# Main Polling Coordinator
# ============================================================================

class MeshPollingDaemon:
    """Main daemon class coordinating all polling operations"""
    
    def __init__(self, config: ConfigManager, once: bool = False):
        self.config = config
        self.once_mode = once
        self.running = False
        self.logger = self._setup_logging()
        
        # Database (MariaDB/MySQL)
        self.db = MySQLAdapter(config)
        
        # Polling configuration
        self.nodelistNode = self.config.get('user-settings', 'nodelistNode', 'localnode.local.mesh')
        self.parallel_threads = self.config.getint('user-settings', 'numParallelThreads', 60)
        self.poller_cycle_minutes = self.config.getint('user-settings', 'pollerCycleTime', 30)
        self.poller_cycle_seconds = max(self.poller_cycle_minutes * 60, 1)
        self.localnode_ip: Optional[str] = None
        self.initial_link_map: Dict[str, Dict] = {}
        
        # Statistics
        self.stats = {
            'numParallelThreads': self.parallel_threads,
            'nodeTotal': 0,
            'garbageReturned': 0,
            'highestHops': 0,
            'totalPolled': 0,
            'nodesWithErrors': 0,
            'noLocation': 0,
            'mappableNodes': 0,
            'mappableLinks': 0,
            'pollingTimeSec': 0.0,
            'babelNodes': 0,
            'olsrNodes': 0,
            'comboNodes': 0,
            'minResponseTimeMs': 0.0,
            'maxResponseTimeMs': 0.0,
        }

        # Firmware classification thresholds (defaults mirror filter.py)
        self.protocol_threshold_seconds = self.config.getint('user-settings', 'protocol_threshold_seconds', 60 * 60 * 24 * 7)
        self.protocol_version_cutoff = version_to_int(str(self.config.get('user-settings', 'protocol_version_cutoff', '3.25.5.0')))
        if self.protocol_version_cutoff is None:
            self.protocol_version_cutoff = version_to_int('3.25.5.0')

        self.protocol_nightly_cutoff = nightly_to_int(str(self.config.get('user-settings', 'protocol_nightly_cutoff', '20250507-aaaaaaaa')))
        if self.protocol_nightly_cutoff is None:
            self.protocol_nightly_cutoff = nightly_to_int('20250507-aaaaaaaa')
        
        # State
        self.session: Optional[aiohttp.ClientSession] = None
        self.node_poller: Optional[NodePoller] = None
        self.nodelistNode_ip: Optional[str] = None
        self.initial_link_map: Dict[str, Dict] = {}
        self.cycle_count: int = 0  # Track cycle number for rate limiting
        self.base_parallel_threads: int = self.parallel_threads  # Store original setting
        
        # Shutdown handling
        self.shutdown_event = asyncio.Event()
        
    def _setup_logging(self) -> logging.Logger:
        """Configure systemd journal logging"""
        # Use a stable logger name; the syslog formatter below still prefixes messages with
        # the script name for journal filtering.
        logger = logging.getLogger('meshmap.poller')
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # Prevent duplicate output
        
        # Detect if running under systemd
        is_systemd = os.environ.get('INVOCATION_ID') is not None
        
        if not is_systemd:
            # Console handler - simple format for CLI
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        # Syslog handler for journalctl - detailed format (systemd only)
        if is_systemd:
            try:
                syslog_handler = SysLogHandler(address='/dev/log')
                syslog_handler.setLevel(logging.INFO)
                syslog_formatter = logging.Formatter(
                    'meshmapPoller.py[%(process)d]: %(levelname)s - %(message)s'
                )
                syslog_handler.setFormatter(syslog_formatter)
                logger.addHandler(syslog_handler)
            except Exception:
                # Fallback to console if syslog unavailable
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(logging.INFO)
                console_formatter = logging.Formatter('%(levelname)s: %(message)s')
                console_handler.setFormatter(console_formatter)
                logger.addHandler(console_handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
        self.shutdown_event.set()

    def _register_signal_handlers(self):
        """Register async-friendly signal handlers"""
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: self._signal_handler(s, None))
        except NotImplementedError:
            # add_signal_handler not supported (e.g., on Windows); fall back silently
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
    
    async def start(self):
        """Start the polling daemon"""
        self.logger.info("=" * 70)
        self.logger.info("Mesh Map Polling Daemon Starting")
        self.logger.info("=" * 70)
        
        try:
            # Register signal handlers now that loop is running
            self._register_signal_handlers()

            # Connect to database
            await self.db.connect()
            
            # Create HTTP session with connection pooling
            connector = aiohttp.TCPConnector(limit=self.parallel_threads, limit_per_host=10)
            self.session = aiohttp.ClientSession(connector=connector)
            self.node_poller = NodePoller(self.session, self.logger)
            
            self.running = True
            
            if self.once_mode:
                self.logger.info("Running in --once mode (single poll cycle)")
                await self._poll_cycle()
                self.logger.info("Poll cycle complete, exiting")
            else:
                self.logger.info(
                    "Running in daemon mode: first cycle full speed, subsequent cycles "
                    f"rate-limited across {self.poller_cycle_minutes} minutes"
                )
                await self._daemon_loop()
                
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def _daemon_loop(self):
        """Main daemon loop"""
        while self.running and not self.shutdown_event.is_set():
            try:
                # Run poll cycle
                await self._poll_cycle()
                    
            except Exception as e:
                self.logger.error(f"Error in poll cycle: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retry
    
    async def _poll_cycle(self):
        """Execute one complete polling cycle"""
        start_time = time.time()
        self.cycle_count += 1
        self.logger.info(f"Starting poll cycle #{self.cycle_count}")
        
        # Step 1: Get localnode info and topology
        self.logger.info(f"Fetching topology from nodelistNode: {self.nodelistNode}")
        topology = await self._fetch_topology()
        if not topology:
            self.logger.error("Failed to retrieve network topology")
            return
        
        # Step 2: Build node device list
        node_devices = self._build_node_list(topology)
        self.stats['nodeTotal'] = len(node_devices)
        self.logger.info(f"Found {self.stats['nodeTotal']} nodes in topology")
        
        # Step 3: Update database with initial topology info
        await self._update_topology_info(node_devices)
        
        # Step 4: Poll all nodes in parallel with rate limiting
        # Use high concurrency for first cycle to get initial data quickly
        if self.cycle_count == 1:
            self.parallel_threads = 600
            self.logger.info(f"Polling {len(node_devices)} nodes at maximum speed ({self.parallel_threads} concurrent)...")
        else:
            self.parallel_threads = self.base_parallel_threads
            self.logger.info(
                f"Polling {len(node_devices)} nodes with rate limiting ({self.parallel_threads} concurrent, "
                f"spread over {self.poller_cycle_minutes} minutes)..."
            )
        polled_nodes = await self._poll_all_nodes(node_devices)

        # Step 6: Calculate statistics
        self._calculate_stats(polled_nodes, node_devices)
        
        # Step 7: Build link topology with distances
        self.logger.info("Building link topology...")
        await self._build_link_topology()
        
        # Step 8: Save statistics
        elapsed = time.time() - start_time
        self.stats['pollingTimeSec'] = round(elapsed, 2)
        
        # Step 9: Generate data files (also updates protocol counts in stats)
        await self._generate_data_files()

        # Step 10: Save statistics after protocol counts are populated
        await self.db.save_polling_stats(self.stats)
        
        self.logger.info(f"Poll cycle completed in {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        self._log_statistics()
    
    async def _fetch_topology(self) -> Optional[Dict]:
        """Fetch node list and neighbor link info using new endpoints"""
        nodes_url = f"http://{self.nodelistNode}/cgi-bin/sysinfo.json?nodes=1"
        lqm_url = f"http://{self.nodelistNode}/cgi-bin/sysinfo.json?lqm=1"
        link_info_url = f"http://{self.nodelistNode}/cgi-bin/sysinfo.json?link_info=1"

        nodes_payload = await self.node_poller.fetch_json(nodes_url)
        lqm_payload = await self.node_poller.fetch_json(lqm_url)
        link_info_payload = await self.node_poller.fetch_json(link_info_url)

        # Extract nodes list
        nodes_list: List[Dict] = []
        if nodes_payload and isinstance(nodes_payload, dict):
            nodes_list = nodes_payload.get('nodes', []) or []

        # Determine nodelistNode IP from interfaces (prefer 10.x mesh address)
        nodelistNode_ip = None
        if nodes_payload and isinstance(nodes_payload, dict):
            interfaces = nodes_payload.get('interfaces', []) or []
            # Prefer br-nomesh 10.x (matches NodePoller default selection) else first 10.x
            for iface in interfaces:
                if isinstance(iface, dict) and iface.get('name') == 'br-nomesh':
                    ip_candidate = iface.get('ip')
                    if ip_candidate and ip_candidate.startswith('10.'):
                        nodelistNode_ip = ip_candidate
                        break
            if not nodelistNode_ip:
                for iface in interfaces:
                    if not isinstance(iface, dict):
                        continue
                    iface_ip = iface.get('ip')
                    if iface_ip and iface_ip != 'none' and iface_ip != 'None':
                        nodelistNode_ip = iface_ip
                        if iface_ip.startswith('10.'):
                            break

        self.nodelistNode_ip = nodelistNode_ip or self.nodelistNode

        # Build link map from LQM (preferred) or link_info (fallback)
        link_map: Dict[str, Dict] = {}

        trackers = None
        if lqm_payload and isinstance(lqm_payload, dict):
            trackers = lqm_payload.get('lqm', {}).get('info', {}).get('trackers')
            if not trackers:
                trackers = lqm_payload.get('lqm', {}).get('trackers')

        if trackers and isinstance(trackers, dict):
            for tracker in trackers.values():
                if not isinstance(tracker, dict):
                    continue
                dest_ip = tracker.get('canonical_ip') or tracker.get('ip')
                if not dest_ip:
                    continue

                link_type = tracker.get('type') or ''
                lt_lower = link_type.lower()
                if lt_lower in ['wireguard', 'tunnel', 'tun']:
                    link_type_out = 'TUN'
                elif lt_lower in ['dtd', 'dtdlink']:
                    link_type_out = 'DTD'
                elif lt_lower == 'rf':
                    link_type_out = 'RF'
                else:
                    link_type_out = link_type.upper() if link_type else 'UNKNOWN'

                link_map[dest_ip] = {
                    'destinationIP': dest_ip,
                    'linkType': link_type_out,
                    'interface': tracker.get('device'),
                    'rxcost': tracker.get('rxcost'),
                    'txcost': tracker.get('txcost'),
                    'rtt': tracker.get('rtt'),
                    'distance': tracker.get('distance'),
                    'quality': tracker.get('quality'),
                    'hostname': tracker.get('hostname'),
                    'lat': tracker.get('lat'),
                    'lon': tracker.get('lon'),
                }

        # Fallback to link_info if no LQM data
        if not link_map and link_info_payload and isinstance(link_info_payload, dict):
            li = link_info_payload.get('link_info')
            if li and isinstance(li, dict):
                for dest_ip, info in li.items():
                    if not isinstance(info, dict):
                        continue
                    link_map[dest_ip] = {
                        'destinationIP': dest_ip,
                        'linkType': info.get('linkType'),
                        'interface': info.get('interface'),
                        'hostname': info.get('hostname'),
                    }

        # Wrap link map by source node (localnode)
        link_map_by_source: Dict[str, Dict] = {}
        if self.nodelistNode_ip and link_map:
            link_map_by_source[self.localnode_ip] = link_map

        # Ensure localnode is present in node list
        if self.localnode_ip:
            already_present = any(n.get('ip') == self.localnode_ip for n in nodes_list)
            if not already_present:
                nodes_list.append({
                    'name': nodes_payload.get('node', self.localnode) if isinstance(nodes_payload, dict) else self.localnode,
                    'ip': self.localnode_ip,
                    'lat': nodes_payload.get('lat') if isinstance(nodes_payload, dict) else None,
                    'lon': nodes_payload.get('lon') if isinstance(nodes_payload, dict) else None,
                    'is_localnode': True,
                })

        if not nodes_list:
            return None

        self.initial_link_map = link_map_by_source

        return {
            'nodes': nodes_list,
            'links': link_map_by_source,
        }

    def _build_node_list(self, topo_bundle: Dict) -> Dict[str, Dict]:
        """Build dictionary of nodes from nodes/list and link map"""
        nodes: Dict[str, Dict] = {}
        max_hops = 0

        node_entries = topo_bundle.get('nodes', []) if topo_bundle else []
        link_map_by_source = topo_bundle.get('links', {}) if topo_bundle else {}

        # First, add all nodes from node_entries
        for node in node_entries:
            if not isinstance(node, dict):
                continue
            ip = node.get('ip')
            if not ip:
                continue
            hops = 1
            if hops > max_hops:
                max_hops = hops
            if node.get('is_localnode'):
                self.localnode_ip = ip
            nodes[ip] = {
                'hopsAway': hops,
                'link_info': link_map_by_source.get(ip, {}),
                'lat': node.get('lat'),
                'lon': node.get('lon'),
                'name': node.get('name'),
                'is_localnode': node.get('is_localnode', False),
            }

        self.stats['highestHops'] = max_hops
        return nodes
    
    async def _update_topology_info(self, node_devices: Dict):
        """Update database with initial topology information"""
        for ip, info in node_devices.items():
            # Only insert pollable nodes (those with valid hopsAway)
            # Synthesized nodes (hopsAway=None) will be created from link data enrichment later
            if info['hopsAway'] is None:
                continue
            node_data = NodeInfo(
                wlan_ip=ip,
                hopsAway=info['hopsAway'],
                link_info=pickle.dumps(info['link_info']).hex()
            )
            try:
                await self.db.upsert_node(node_data)
            except Exception as e:
                self.logger.error(f"Error updating topology for {ip}: {e}")
    
    async def _poll_all_nodes(self, node_devices: Dict) -> List[NodeInfo]:
        """Poll all nodes with rate limiting; responsive to shutdown"""
        semaphore = asyncio.Semaphore(self.parallel_threads)
        tasks: List[asyncio.Task] = []
        
        # Calculate delay between task creation for cycles after the first
        total_nodes = len(node_devices)
        if self.cycle_count > 1 and total_nodes > 0:
            # Spread polls evenly across configured pollerCycleTime window
            inter_poll_delay = self.poller_cycle_seconds / total_nodes
            self.logger.info(
                f"Spreading {total_nodes} nodes over {self.poller_cycle_minutes} minutes: "
                f"{inter_poll_delay:.3f}s between polls"
            )
        else:
            inter_poll_delay = 0.0

        async def rate_limited_poll(ip: str, hops: int, delay: float):
            if self.shutdown_event.is_set():
                return None
            # Skip synthesized nodes (hops=None means discovered via links only, not directly reachable)
            if hops is None:
                self.logger.debug(f"Skipping synthesized node {ip} (not directly reachable)")
                return None
            # Add startup delay for rate limiting (after first cycle)
            if delay > 0:
                await asyncio.sleep(delay)
            async with semaphore:
                if self.shutdown_event.is_set():
                    return None
                return await self.node_poller.poll_node(ip, hops)

        # Create tasks for all nodes with staggered delays
        for idx, (ip, info) in enumerate(node_devices.items()):
            # Skip synthesized nodes - they're discovered via link data but not directly reachable
            if info['hopsAway'] is None:
                continue
            delay = inter_poll_delay * idx if self.cycle_count > 1 else 0.0
            tasks.append(asyncio.create_task(rate_limited_poll(ip, info['hopsAway'], delay)))

        results: List[NodeInfo] = []
        completed = 0
        total = len(tasks)

        pending = set(tasks)
        try:
            while pending and not self.shutdown_event.is_set():
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED
                )
                for t in done:
                    if t.cancelled():
                        continue
                    try:
                        result = t.result()
                    except Exception as e:
                        self.logger.debug(f"Polling task error: {e}")
                        self.stats['nodesWithErrors'] += 1
                        continue

                    completed += 1
                    if completed % 10 == 0 or completed == total:
                        percent = int((completed / total) * 100)
                        self.logger.info(f"Polling progress: {percent}% ({completed}/{total})")

                    if result:
                        results.append(result)
                        try:
                            await self.db.upsert_node(result)
                        except Exception as e:
                            self.logger.error(f"Error saving node {result.wlan_ip}: {e}")
                    else:
                        self.stats['nodesWithErrors'] += 1
        finally:
            # Cancel any remaining tasks on shutdown
            for t in pending:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        return results
    
    def _calculate_stats(self, nodes: List[NodeInfo], node_devices: Dict[str, Dict] = None):
        """Calculate polling statistics"""
        self.stats['totalPolled'] = len(nodes)
        
        # Calculate garbage returned: nodes we tried to poll but failed
        # This is nodes that should have been polled (not synthesized) minus those that succeeded
        if node_devices:
            pollable_nodes = sum(1 for info in node_devices.values() if info['hopsAway'] is not None)
            synthesized_nodes = len(node_devices) - pollable_nodes
            self.stats['garbageReturned'] = pollable_nodes - len(nodes)
            self.logger.info(f"Pollable nodes: {pollable_nodes}, Synthesized: {synthesized_nodes}, Successfully polled: {len(nodes)}")
        else:
            self.stats['garbageReturned'] = self.stats['nodeTotal'] - len(nodes)
        
        no_location = sum(1 for n in nodes if n.lat == 0.0 or n.lon == 0.0)
        no_location_nodes = [n.node for n in nodes if n.lat == 0.0 or n.lon == 0.0]
        if no_location_nodes:
            self.logger.info(f"Nodes with no location (lat/lon = 0): {no_location_nodes}")
        self.stats['noLocation'] = no_location
        self.stats['mappableNodes'] = len(nodes) - no_location
        
        # Calculate response time statistics
        if nodes:
            response_times = [n.response_time_ms for n in nodes if n.response_time_ms > 0]
            if response_times:
                self.stats['minResponseTimeMs'] = round(min(response_times), 2)
                self.stats['maxResponseTimeMs'] = round(max(response_times), 2)
    
    async def _build_link_topology(self):
        """Build complete link topology with distance/bearing calculations"""
        try:
            all_nodes = await self.db.get_all_nodes()
            link_count = 0
            
            for node in all_nodes:
                links = None

                # Prefer stored link_info; fallback to initial LQM map for localnode
                if node.get('link_info'):
                    try:
                        links = pickle.loads(bytes.fromhex(node['link_info']))
                    except Exception:
                        links = None
                elif self.localnode_ip and node.get('wlan_ip') == self.localnode_ip:
                    links = self.initial_link_map.get(self.localnode_ip, {})

                if not isinstance(links, dict) or not links:
                    continue
                
                node_lat = float(node.get('lat', 0))
                node_lon = float(node.get('lon', 0))
                
                if node_lat == 0 or node_lon == 0:
                    continue
                
                # Enrich each link with coordinates and distance
                for dest_ip, link_data in links.items():
                    # Find destination node coordinates
                    dest_node = next((n for n in all_nodes if n.get('wlan_ip') == dest_ip), None)
                    dest_lat = 0.0
                    dest_lon = 0.0

                    if dest_node:
                        dest_lat = float(dest_node.get('lat', 0) or 0)
                        dest_lon = float(dest_node.get('lon', 0) or 0)

                    # Fallback to link-provided coordinates (from LQM tracker)
                    if (not dest_lat or not dest_lon) and link_data.get('lat') and link_data.get('lon'):
                        dest_lat = float(link_data.get('lat'))
                        dest_lon = float(link_data.get('lon'))
                        # If we had no DB node for this IP, synthesize one to enrich later lookups
                        if not dest_node:
                            dest_node = {'wlan_ip': dest_ip, 'lat': dest_lat, 'lon': dest_lon}

                    if dest_lat == 0 or dest_lon == 0:
                        continue
                    
                    link_data['linkLat'] = dest_lat
                    link_data['linkLon'] = dest_lon
                    
                    # Calculate distance and bearing for RF links
                    if link_data.get('linkType') == 'RF':
                        dist_bear = self._calculate_distance_bearing(
                            node_lat, node_lon, dest_lat, dest_lon
                        )
                        link_data.update(dist_bear)
                    
                    link_count += 1
                
                # Update database with enriched links
                await self.db.update_link_info(node['wlan_ip'], links)
            
            self.stats['mappableLinks'] = link_count
            self.logger.info(f"Built {link_count} mappable links")
            
        except Exception as e:
            self.logger.error(f"Error building link topology: {e}", exc_info=True)
    
    @staticmethod
    def _calculate_distance_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> Dict:
        """Calculate distance and bearing between two coordinates"""
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        # Haversine formula for distance
        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance_km = 6371 * c  # Earth radius in km
        distance_miles = distance_km * 0.621371
        
        # Calculate bearing
        y = math.sin(delta_lon) * math.cos(lat2_rad)
        x = (math.cos(lat1_rad) * math.sin(lat2_rad) -
             math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon))
        bearing = (math.degrees(math.atan2(y, x)) + 360) % 360
        
        return {
            'distanceKM': round(distance_km, 2),
            'distanceMiles': round(distance_miles, 2),
            'bearing': round(bearing, 1)
        }
    
    async def _generate_data_files(self):
        """Generate JavaScript/JSON data files for web interface"""
        try:
            data_dir = Path(self.config.get('user-settings', 'webpageDataDir', 'data'))
            data_dir.mkdir(parents=True, exist_ok=True)
            
            # Get all nodes from database
            all_nodes = await self.db.get_all_nodes()
            
            # Organize devices by frequency band (matching PHP createJS.inc logic)
            all_devices = {
                'noRF': [],
                'supernode': [],
                '900': [],
                '2ghz': [],
                '3ghz': [],
                '5ghz': []
            }
            
            node_report = []

            babel_count = 0
            olsr_count = 0
            combo_count = 0
            
            for node in all_nodes:
                # Only include nodes with valid location data
                lat = node.get('lat', 0)
                lon = node.get('lon', 0)
                
                # Build node data for reporting
                # Convert datetime to ISO 8601 UTC string
                last_seen_raw = node.get('last_seen', '')
                protocol = self._determine_protocol(node.get('firmware_version', ''), last_seen_raw)

                # Convert to ISO 8601 UTC format for frontend
                last_seen = _to_iso8601_utc(last_seen_raw)
                
                # Deserialize link_info from pickle if it's stored as hex string
                link_info_data = {}
                link_info_raw = node.get('link_info', '')
                if link_info_raw and isinstance(link_info_raw, str):
                    try:
                        link_info_data = pickle.loads(bytes.fromhex(link_info_raw))
                    except:
                        link_info_data = {}
                elif isinstance(link_info_raw, dict):
                    link_info_data = link_info_raw

                # Fallback: if this is the localnode and DB lacked link_info, use the in-memory initial map
                if (not link_info_data):
                    if self.localnode_ip and node.get('wlan_ip') == self.localnode_ip:
                        link_info_data = self.initial_link_map.get(self.localnode_ip, {})
                    elif self.initial_link_map and node.get('node') == self.config.get('user-settings', 'localnode', 'localnode.local.mesh'):
                        # Fallback match by node name if IPs differ
                        # initial_link_map is keyed by localnode_ip; grab first (only) entry
                        first_entry = next(iter(self.initial_link_map.values()), {})
                        link_info_data = first_entry
                
                # Deserialize services from pickle if it's stored as hex string
                services_data = []
                services_raw = node.get('services', 'Not Available')
                if services_raw and isinstance(services_raw, str) and services_raw != 'Not Available':
                    try:
                        services_data = pickle.loads(bytes.fromhex(services_raw))
                        # Ensure it's a list
                        if not isinstance(services_data, list):
                            services_data = []
                    except:
                        services_data = []
                elif isinstance(services_raw, list):
                    services_data = services_raw
                
                # Deserialize loadavg from pickle if it's stored as hex string
                loadavg_data = [0, 0, 0]
                loadavg_raw = node.get('loadavg', '')
                if loadavg_raw and isinstance(loadavg_raw, str):
                    try:
                        loadavg_data = pickle.loads(bytes.fromhex(loadavg_raw))
                        # Ensure it's a list with 3 elements
                        if not isinstance(loadavg_data, list) or len(loadavg_data) != 3:
                            loadavg_data = [0, 0, 0]
                    except:
                        loadavg_data = [0, 0, 0]
                elif isinstance(loadavg_raw, list):
                    if len(loadavg_raw) == 3:
                        loadavg_data = loadavg_raw
                    else:
                        loadavg_data = [0, 0, 0]
                
                # Clean description: remove HTML br tags and replace with space
                description = node.get('description', '')
                if description:
                    import re
                    description = re.sub(r'<br\s*/?>', ' ', description, flags=re.IGNORECASE)
                
                node_data = {
                    'node': node.get('node', ''),
                    'wlan_ip': node.get('wlan_ip', ''),
                    'lat': lat,
                    'lon': lon,
                    'description': description,
                    'grid_square': node.get('grid_square', ''),
                    'model': node.get('model', ''),
                    'firmware_version': node.get('firmware_version', ''),
                    'uptime': node.get('uptime', 'Not Available'),
                    'loadavg': loadavg_data,
                    'ssid': node.get('ssid', 'None'),
                    'channel': node.get('channel', 'None'),
                    'chanbw': node.get('chanbw', 'None'),
                    'freq': node.get('freq', 'None'),
                    'active_tunnel_count': node.get('active_tunnel_count', '0'),
                    'firmware_mfg': node.get('firmware_mfg', 'Not Available'),
                    'board_id': node.get('board_id', 'Not Available'),
                    'services': services_data if isinstance(services_data, list) else 'Not Available',
                    'link_info': link_info_data,
                    'antGain': node.get('antGain', 0),
                    'antBeam': node.get('antBeam', 0),
                    'antDesc': node.get('antDesc', 'Not Available'),
                    'mesh_supernode': node.get('mesh_supernode', 'false'),
                    'mesh_gateway': node.get('mesh_gateway', 'false'),
                    'last_seen': last_seen,
                    'protocol': protocol,
                    'response_time_ms': int(round(node.get('response_time_ms', 0.0))),
                }
                
                node_report.append(node_data)

                # Track protocol counts for stats
                if protocol == 'Babel Only':
                    babel_count += 1
                elif protocol == 'OLSR Only':
                    olsr_count += 1
                elif protocol == 'Combo':
                    combo_count += 1
                
                # Categorize by frequency band if location is available
                if lat != 0 or lon != 0:
                    is_supernode = node.get('mesh_supernode', 'false') == 'true'
                    is_no_rf = node.get('meshRF', 'on') == 'off' or node.get('channel', 'none') == 'none'
                    channel = node.get('channel', 'none')
                    board_id = node.get('board_id', '')
                    
                    if is_supernode:
                        all_devices['supernode'].append(node_data)
                    elif is_no_rf:
                        all_devices['noRF'].append(node_data)
                    elif channel == 'none':
                        all_devices['noRF'].append(node_data)
                    elif board_id in ['0xe009', '0xe1b9', '0xe239']:  # 900MHz boards
                        all_devices['900'].append(node_data)
                    elif isinstance(channel, str) and channel.isdigit():
                        ch = int(channel)
                        if ch <= 11:  # 2.4GHz channels 1-11
                            all_devices['2ghz'].append(node_data)
                        elif (37 <= ch <= 64) or (100 <= ch <= 184) or ch >= 3000:  # 5GHz channels or 6GHz
                            all_devices['5ghz'].append(node_data)
                        elif 76 <= ch <= 99:  # 3GHz channels 76-99
                            all_devices['3ghz'].append(node_data)
                    elif isinstance(channel, int):
                        if channel <= 11:  # 2.4GHz channels 1-11
                            all_devices['2ghz'].append(node_data)
                        elif (37 <= channel <= 64) or (100 <= channel <= 184) or channel >= 3000:  # 5GHz channels or 6GHz
                            all_devices['5ghz'].append(node_data)
                        elif 76 <= channel <= 99:  # 3GHz channels 76-99
                            all_devices['3ghz'].append(node_data)
            
            # Generate map_data.js with all required variables
            # Count nodes for statistics
            total_nodes_in_db = len([n for n in all_nodes if n.get('lat') or n.get('lon')])
            week_plus_old = len([n for n in all_nodes if not n.get('last_seen')])
            
            # Parse map tile servers from user config only (not defaults)
            # This respects commented-out lines in settings.ini
            map_tile_servers = {}
            default_tile_server = None
            priority_list: List[str] = []
            user_config = configparser.ConfigParser()
            # Preserve case so keys like inet.Topographic remain intact
            user_config.optionxform = str
            user_config.read(Path('../settings.ini'))
            
            if 'user-settings' in user_config:
                # Parse tileServerPriority list (preferred ordering and default)
                priority_raw = user_config.get('user-settings', 'tileServerPriority', fallback='')
                if priority_raw:
                    try:
                        priority_list = json.loads(priority_raw)
                    except Exception:
                        try:
                            import ast
                            parsed = ast.literal_eval(priority_raw)
                            if isinstance(parsed, list):
                                priority_list = [str(x) for x in parsed]
                        except Exception:
                            priority_list = []
                priority_list = [p.strip().strip('"').strip("'") for p in priority_list if p]

                # Read DefaultTileServer (legacy)
                default_tile_server = user_config.get('user-settings', 'DefaultTileServer', fallback='')
                if default_tile_server.startswith('"') and default_tile_server.endswith('"'):
                    default_tile_server = default_tile_server[1:-1]
                if not default_tile_server:
                    default_tile_server = None

                # Collect all internet tile servers
                for key in user_config['user-settings']:
                    if key.lower().startswith('inettileserver['):
                        inet_name = key.split("'")[1] if "'" in key else key.split('[')[1].split(']')[0]
                        inet_value = user_config.get('user-settings', key)
                        if inet_value.startswith('"') and inet_value.endswith('"'):
                            inet_value = inet_value[1:-1]
                        map_tile_servers[inet_name] = inet_value

                # Collect all AREDN tile servers
                for key in user_config['user-settings']:
                    if key.lower().startswith('aredntileserver['):
                        aredn_name = key.split("'")[1] if "'" in key else key.split('[')[1].split(']')[0]
                        aredn_value = user_config.get('user-settings', key)
                        if aredn_value.startswith('"') and aredn_value.endswith('"'):
                            aredn_value = aredn_value[1:-1]
                        map_tile_servers[aredn_name] = aredn_value

                # Legacy mapTileServers support
                for key in user_config['user-settings']:
                    if key.lower().startswith('maptileservers['):
                        server_name = key.split("'")[1] if "'" in key else key.split('[')[1].split(']')[0]
                        value = user_config.get('user-settings', key)
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        map_tile_servers[server_name] = value

                # Determine default from priority list first
                if priority_list:
                    for candidate in priority_list:
                        if candidate in map_tile_servers:
                            default_tile_server = candidate
                            break

                # Fallbacks if still not set: legacy keys, then legacy suffix, then first entry
                if not default_tile_server:
                    inet_default = user_config.get('user-settings', 'inetDefaultTileServer', fallback='')
                    if inet_default.startswith('"') and inet_default.endswith('"'):
                        inet_default = inet_default[1:-1]
                    if inet_default and inet_default in map_tile_servers:
                        default_tile_server = inet_default

                if not default_tile_server:
                    aredn_default = user_config.get('user-settings', 'arednDefaultTileServer', fallback='')
                    if aredn_default.startswith('"') and aredn_default.endswith('"'):
                        aredn_default = aredn_default[1:-1]
                    if aredn_default and aredn_default in map_tile_servers:
                        default_tile_server = aredn_default

                if not default_tile_server:
                    for name in map_tile_servers:
                        if name.endswith('-default') or name.endswith('-Default'):
                            default_tile_server = name
                            break

                if not default_tile_server and map_tile_servers:
                    default_tile_server = next(iter(map_tile_servers))

                # Reorder map_tile_servers honoring priority_list first
                if priority_list:
                    ordered = {}
                    for name in priority_list:
                        if name in map_tile_servers:
                            ordered[name] = map_tile_servers[name]
                    for name, url in map_tile_servers.items():
                        if name not in ordered:
                            ordered[name] = url
                    map_tile_servers = ordered
            
            # Parse map center coordinates
            map_center_lat = float(self.config.get('user-settings', "map_center_coordinates['lat']", '0'))
            map_center_lon = float(self.config.get('user-settings', "map_center_coordinates['lon']", '0'))
            
            # Handle distanceUnits setting (default to miles if missing or invalid)
            distance_units = self.config.get('user-settings', 'distanceUnits', 'miles')
            kilometers = distance_units == 'kilometers'
            
            map_info = {
                'localnode': self.nodelistNode,
                'lastUpdate': datetime.utcnow().replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'mapTileServers': map_tile_servers,
                'defaultTileServer': default_tile_server,
                'title': self.config.get('user-settings', 'map_browserTitle', 'MeshMap'),
                'attribution': self.config.get('user-settings', 'attribution', ''),
                'mapContact': self.config.get('user-settings', 'mapContact', ''),
                'kilometers': kilometers,
                'webpageDataDir': '',
                'mapCenterCoords': [map_center_lat, map_center_lon],
                'mapInitialZoom': int(self.config.get('user-settings', 'map_initial_zoom_level', '10')),
                'totalNodesInDB': total_nodes_in_db,
                'weekPlusOld': week_plus_old,
                'reportBrowserTitle': self.config.get('user-settings', 'report_browserTitle', 'Node Report'),
                'reportPageTitle': self.config.get('user-settings', 'report_pageTitle', 'Node Report')
            }
            
            # Custom JSON encoder to handle Decimal types from MariaDB
            def decimal_default(obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            # Create map_data.json with all required data
            # Update protocol counts in stats before emitting outputs
            self.stats['babelNodes'] = babel_count
            self.stats['olsrNodes'] = olsr_count
            self.stats['comboNodes'] = combo_count

            map_data = {
                "mapInfo": map_info,
                "pollingInfo": self.stats,
                "allDevices": all_devices
            }
            
            map_data_file = data_dir / 'map_data.json'
            map_data_file.write_text(json.dumps(map_data, indent=2, default=decimal_default))
            
            # Generate node_report_data.json
            node_report_file = data_dir / 'node_report_data.json'
            node_report_file.write_text(json.dumps(node_report, indent=2, default=decimal_default))
            
            self.logger.info(f"Generated data files in {data_dir}")
            
        except Exception as e:
            self.logger.error(f"Error generating data files: {e}", exc_info=True)
    
    def _log_statistics(self):
        """Log polling statistics"""
        self.logger.info("=" * 70)
        self.logger.info("POLLING STATISTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Total Node Count: {self.stats['nodeTotal']}")
        self.logger.info(f"Garbage Returned: {self.stats['garbageReturned']}")
        self.logger.info(f"Highest Hops Away: {self.stats['highestHops']}")
        self.logger.info(f"Total Nodes Polled: {self.stats['totalPolled']}")
        self.logger.info(f"Nodes with Errors: {self.stats['nodesWithErrors']}")
        self.logger.info(f"Nodes with No Location: {self.stats['noLocation']}")
        self.logger.info(f"Total Mappable on Map: {self.stats['mappableNodes']}")
        self.logger.info(f"Links Found: {self.stats['mappableLinks']}")
        self.logger.info(f"Babel Nodes: {self.stats.get('babelNodes', 0)}")
        self.logger.info(f"OLSR Nodes: {self.stats.get('olsrNodes', 0)}")
        self.logger.info(f"Combo Nodes: {self.stats.get('comboNodes', 0)}")
        self.logger.info(f"Minimum Response Time: {self.stats.get('minResponseTimeMs', 0):.2f}ms")
        self.logger.info(f"Maximum Response Time: {self.stats.get('maxResponseTimeMs', 0):.2f}ms")
        self.logger.info(f"Polling Time: {self.stats['pollingTimeSec']:.2f}s ({self.stats['pollingTimeSec']/60:.2f}m)")
        self.logger.info("=" * 70)
    
    async def cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up resources...")
        
        if self.session:
            await self.session.close()
        
        if self.db:
            await self.db.close()
        
        self.logger.info("Shutdown complete")

    def _determine_protocol(self, firmware_version: str, last_seen_value: Any) -> str:
        """Classify node protocol as Babel Only / OLSR Only / Combo / Unknown."""
        last_seen_ts = _parse_last_seen(last_seen_value)
        if last_seen_ts is None:
            return "Unknown"

        if last_seen_ts < time.time() - self.protocol_threshold_seconds:
            return "Unknown"

        if _is_firmware(firmware_version, 'babel', self.protocol_version_cutoff, self.protocol_nightly_cutoff):
            return "Babel Only"
        if _is_firmware(firmware_version, 'olsr', self.protocol_version_cutoff, self.protocol_nightly_cutoff):
            return "OLSR Only"
        if _is_firmware(firmware_version, 'combo', self.protocol_version_cutoff, self.protocol_nightly_cutoff):
            return "Combo"

        return "Unknown"


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Mesh Map Polling Daemon',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    Run in daemon mode (continuous polling)
  %(prog)s --once             Run one poll cycle then exit
  %(prog)s --flush            Clear the node_info database table and exit
  
For systemd integration, use the provided pollingScript.service file.
        """
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Perform one poll cycle then exit cleanly'
    )
    parser.add_argument(
        '--flush',
        action='store_true',
        help='Clear the node_info database table and exit'
    )
    parser.add_argument(
        '--config',
        default='../settings.ini',
        help='Path to configuration file (default: ../settings.ini)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = ConfigManager(args.config)
        
        # Handle --flush flag
        if args.flush:
            asyncio.run(_flush_database(config))
            print("Database flushed successfully")
            sys.exit(0)
        
        # Create and run daemon
        daemon = MeshPollingDaemon(config, once=args.once)
        asyncio.run(daemon.start())
        
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(0)
    except asyncio.CancelledError:
        print("\nCancelled")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


async def _flush_database(config: ConfigManager):
    """Flush and reinitialize MariaDB database from scratch"""
    import subprocess
    import aiomysql
    
    # Get DB config
    sql_server = config.get('user-settings', 'sql_server', 'localhost')
    sql_user = config.get('user-settings', 'sql_user', 'mesh-map')
    sql_passwd = config.get('user-settings', 'sql_passwd', 'password')
    sql_db = config.get('user-settings', 'sql_db', 'node_map')
    sql_db_tbl_node = config.get('user-settings', 'sql_db_tbl_node', 'node_info')
    sql_db_tbl_map = config.get('user-settings', 'sql_db_tbl_map', 'map_info')
    sql_db_tbl_aredn = config.get('user-settings', 'sql_db_tbl_aredn', 'aredn_info')
    
    print(f"Initializing MariaDB database '{sql_db}' with user '{sql_user}'...")
    
    try:
        # Use sudo mariadb for root access (socket authentication)
        print(f"Creating database '{sql_db}'...")
        subprocess.run([
            'sudo', 'mariadb', '-e',
            f"CREATE DATABASE IF NOT EXISTS `{sql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        ], check=True, capture_output=True, text=True)
        
        print(f"Setting up user '{sql_user}'...")
        subprocess.run([
            'sudo', 'mariadb', '-e',
            f"CREATE USER IF NOT EXISTS '{sql_user}'@'localhost' IDENTIFIED BY '{sql_passwd}';"
        ], check=True, capture_output=True, text=True)
        
        subprocess.run([
            'sudo', 'mariadb', '-e',
            f"GRANT ALL PRIVILEGES ON `{sql_db}`.* TO '{sql_user}'@'localhost'; FLUSH PRIVILEGES;"
        ], check=True, capture_output=True, text=True)
        
        print("Database and user configured successfully.")
        
        # Now connect as the application user to create tables
        user_conn = await aiomysql.connect(
            host=sql_server,
            user=sql_user,
            password=sql_passwd,
            db=sql_db,
            autocommit=True
        )
        
        async with user_conn.cursor() as cur:
            # Drop existing tables
            print(f"Dropping existing tables...")
            await cur.execute(f"DROP TABLE IF EXISTS `{sql_db_tbl_node}`")
            await cur.execute(f"DROP TABLE IF EXISTS `{sql_db_tbl_map}`")
            await cur.execute(f"DROP TABLE IF EXISTS `{sql_db_tbl_aredn}`")
            
            # Create node_info table
            print(f"Creating table '{sql_db_tbl_node}'...")
            await cur.execute(f"""
                CREATE TABLE `{sql_db_tbl_node}` (
                    `wlan_ip` VARCHAR(45) PRIMARY KEY,
                    `node` VARCHAR(255) DEFAULT NULL,
                    `uptime` VARCHAR(255) DEFAULT NULL,
                    `loadavg` VARCHAR(255) DEFAULT NULL,
                    `model` VARCHAR(255) DEFAULT NULL,
                    `firmware_version` VARCHAR(50) DEFAULT NULL,
                    `ssid` VARCHAR(255) DEFAULT NULL,
                    `channel` VARCHAR(50) DEFAULT NULL,
                    `chanbw` VARCHAR(50) DEFAULT NULL,
                    `tunnel_installed` VARCHAR(10) DEFAULT 'false',
                    `active_tunnel_count` VARCHAR(10) DEFAULT '0',
                    `lat` DECIMAL(12,7) DEFAULT 0.0,
                    `lon` DECIMAL(13,7) DEFAULT 0.0,
                    `wifi_mac_address` VARCHAR(17) DEFAULT NULL,
                    `api_version` VARCHAR(50) DEFAULT NULL,
                    `board_id` VARCHAR(50) DEFAULT NULL,
                    `firmware_mfg` VARCHAR(100) DEFAULT NULL,
                    `grid_square` VARCHAR(50) DEFAULT NULL,
                    `lan_ip` VARCHAR(45) DEFAULT NULL,
                    `services` TEXT DEFAULT NULL,
                    `description` TEXT DEFAULT NULL,
                    `mesh_supernode` VARCHAR(10) DEFAULT 'false',
                    `mesh_gateway` VARCHAR(10) DEFAULT 'false',
                    `freq` VARCHAR(50) DEFAULT NULL,
                    `link_info` MEDIUMTEXT DEFAULT NULL,
                    `hopsAway` INT DEFAULT 0,
                    `meshRF` VARCHAR(10) DEFAULT 'on',
                    `last_seen` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    `antGain` DECIMAL(5,2) DEFAULT 0,
                    `antBeam` DECIMAL(5,2) DEFAULT 0,
                    `antDesc` VARCHAR(255) DEFAULT NULL,
                    `antBuiltin` VARCHAR(10) DEFAULT 'false',
                    `response_time_ms` FLOAT DEFAULT 0.0,
                    INDEX `idx_node` (`node`),
                    INDEX `idx_hops` (`hopsAway`),
                    INDEX `idx_last_seen` (`last_seen`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Add response_time_ms column if it doesn't exist (migration for existing databases)
            print(f"Ensuring response_time_ms column exists in '{sql_db_tbl_node}'...")
            try:
                await cur.execute(f"""
                    ALTER TABLE `{sql_db_tbl_node}` 
                    ADD COLUMN `response_time_ms` FLOAT DEFAULT 0.0
                """)
                print("Column response_time_ms added successfully.")
            except Exception as e:
                # Column likely already exists, that's fine
                if "Duplicate column name" not in str(e):
                    print(f"Note: Could not add response_time_ms column: {e}")

            
            # Create map_info table
            print(f"Creating table '{sql_db_tbl_map}'...")
            await cur.execute(f"""
                CREATE TABLE `{sql_db_tbl_map}` (
                    `id` VARCHAR(50) PRIMARY KEY,
                    `numParallelThreads` INT DEFAULT 0,
                    `nodeTotal` INT DEFAULT 0,
                    `garbageReturned` INT DEFAULT 0,
                    `highestHops` INT DEFAULT 0,
                    `totalPolled` INT DEFAULT 0,
                    `noLocation` INT DEFAULT 0,
                    `mappableNodes` INT DEFAULT 0,
                    `mappableLinks` INT DEFAULT 0,
                    `pollingTimeSec` DECIMAL(10,2) DEFAULT 0,
                    `lastPollingRun` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            
            # Create aredn_info table
            print(f"Creating table '{sql_db_tbl_aredn}'...")
            await cur.execute(f"""
                CREATE TABLE `{sql_db_tbl_aredn}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `version_type` VARCHAR(50) DEFAULT NULL,
                    `version` VARCHAR(50) DEFAULT NULL,
                    `updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
        
        user_conn.close()
        print(f"Database '{sql_db}' initialized successfully with all tables.")
        
    except subprocess.CalledProcessError as e:
        print(f"MariaDB command error: {e.stderr}", file=sys.stderr)
        raise
    except aiomysql.Error as e:
        print(f"MariaDB connection error: {e}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"Error during database flush: {e}", file=sys.stderr)
        raise


if __name__ == '__main__':
    main()
