#
# Copyright 2025 The TadoLocal and AmpScm contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Background scheduler service for zone climate schedules."""

import asyncio
import datetime
import json
import logging
import sqlite3
import time
from typing import Dict, List, Optional, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def get_timezone(db_path: str) -> Optional[str]:
    """
    Get timezone from database.
    
    Returns the timezone string from tado_homes table, or None if not configured.
    """
    try:
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute("""
                SELECT timezone FROM tado_homes
                LIMIT 1
            """)
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
            return None
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Failed to get timezone from database: {e}")
        return None


def utc_to_local(utc_dt: datetime.datetime, timezone_str: str) -> datetime.datetime:
    """
    Convert UTC datetime to local timezone.
    
    Args:
        utc_dt: UTC datetime (naive or timezone-aware)
        timezone_str: Timezone string (e.g., 'Europe/Amsterdam')
    
    Returns:
        Local datetime (timezone-aware)
    """
    try:
        tz = ZoneInfo(timezone_str)
        # If datetime is naive, assume it's UTC
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=datetime.timezone.utc)
        # Convert to local timezone
        return utc_dt.astimezone(tz)
    except Exception as e:
        logger.warning(f"Failed to convert UTC to local timezone {timezone_str}: {e}, using UTC")
        # Fallback: return UTC datetime
        if utc_dt.tzinfo is None:
            return utc_dt.replace(tzinfo=datetime.timezone.utc)
        return utc_dt


def get_local_now(db_path: str) -> datetime.datetime:
    """
    Get current local time based on configured timezone.
    
    Returns timezone-aware datetime in local timezone, or UTC if timezone not configured.
    """
    timezone_str = get_timezone(db_path)
    
    # Get current UTC time
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    
    if timezone_str:
        try:
            # Validate timezone
            ZoneInfo(timezone_str)
            # Convert to local time
            return utc_to_local(utc_now, timezone_str)
        except Exception as e:
            logger.warning(f"Invalid timezone '{timezone_str}': {e}, using UTC")
            return utc_now
    
    # No timezone configured, use UTC
    return utc_now


def round_to_5_minutes(dt: datetime.datetime) -> datetime.datetime:
    """Round datetime down to nearest 5-minute interval."""
    minutes = dt.minute
    rounded_minutes = (minutes // 5) * 5
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)


def parse_time(time_str: str) -> tuple:
    """Parse HH:MM time string to (hour, minute) tuple."""
    parts = time_str.split(':')
    return (int(parts[0]), int(parts[1]))


def day_matches_week_weekends(days_of_week: str, current_day: int) -> bool:
    """
    Check if current day matches week/weekends schedule.
    
    Args:
        days_of_week: 'weekdays' or 'weekends'
        current_day: 0=Monday, 6=Sunday (Python weekday)
    """
    if days_of_week == 'weekdays':
        # Monday=0 to Friday=4
        return current_day < 5
    elif days_of_week == 'weekends':
        # Saturday=5, Sunday=6
        return current_day >= 5
    return False


def matches_schedule(schedule: Dict, current_time: datetime.datetime, current_day: int) -> bool:
    """
    Check if schedule matches current time and day.
    
    Args:
        schedule: Schedule dict with 'schedule_type', 'days_of_week', 'time', 'enabled'
        current_time: Current datetime
        current_day: Current weekday (0=Monday, 6=Sunday)
    """
    # Check if schedule is enabled
    if not schedule.get('enabled', True):
        return False
    
    # Round current time to nearest 5-minute interval
    current_rounded = round_to_5_minutes(current_time)
    schedule_time = parse_time(schedule['time'])
    
    # Check if time matches (must match exactly at 5-minute intervals)
    if (current_rounded.hour, current_rounded.minute) != schedule_time:
        return False
    
    # Check day based on schedule type
    schedule_type = schedule['schedule_type']
    days_of_week = schedule['days_of_week']
    
    if schedule_type == 'any_day':
        return True
    elif schedule_type == 'week_weekends':
        return day_matches_week_weekends(days_of_week, current_day)
    else:  # day_of_week
        try:
            days_list = json.loads(days_of_week)
            return current_day in days_list
        except (json.JSONDecodeError, TypeError):
            return False


def get_current_schedule_temperature(db_path: str, zone_id: int) -> Optional[float]:
    """
    Get the temperature from the current/relevant schedule for a zone.
    
    Returns the temperature from the schedule that matches the current time/day,
    or None if no schedule matches.
    """
    import sqlite3
    now = get_local_now(db_path)
    current_day = now.weekday()  # 0=Monday, 6=Sunday
    current_time_rounded = round_to_5_minutes(now)
    
    conn = sqlite3.connect(db_path)
    try:
        # Get all enabled schedules for this zone
        cursor = conn.execute("""
            SELECT schedule_id, schedule_type, days_of_week, time, temperature
            FROM zone_schedules
            WHERE zone_id = ? AND enabled = 1
            ORDER BY time DESC
        """, (zone_id,))
        
        schedules = []
        for row in cursor.fetchall():
            schedules.append({
                'schedule_id': row[0],
                'schedule_type': row[1],
                'days_of_week': row[2],
                'time': row[3],
                'temperature': row[4],
                'enabled': True
            })
        
        # Find the schedule that matches current time/day
        # If multiple match, use the one with the latest time
        matching_schedules = []
        for schedule in schedules:
            if matches_schedule(schedule, now, current_day):
                matching_schedules.append(schedule)
        
        if matching_schedules:
            # Return temperature from the first matching schedule
            # (schedules are ordered by time DESC, so first is most recent)
            return matching_schedules[0]['temperature']
        
        # If no schedule matches current time, find the most recent past schedule for today
        # This gives us the "last known schedule" temperature
        today_schedules = []
        for schedule in schedules:
            # Check if schedule applies to today
            schedule_type = schedule['schedule_type']
            days_of_week = schedule['days_of_week']
            
            applies_today = False
            if schedule_type == 'any_day':
                applies_today = True
            elif schedule_type == 'week_weekends':
                applies_today = day_matches_week_weekends(days_of_week, current_day)
            else:  # day_of_week
                try:
                    days_list = json.loads(days_of_week)
                    applies_today = current_day in days_list
                except (json.JSONDecodeError, TypeError):
                    applies_today = False
            
            if applies_today:
                # Parse schedule time
                schedule_time = parse_time(schedule['time'])
                schedule_datetime = now.replace(hour=schedule_time[0], minute=schedule_time[1], second=0, microsecond=0)
                
                # Check if this schedule time has passed today
                if schedule_datetime <= current_time_rounded:
                    today_schedules.append((schedule_datetime, schedule))
        
        if today_schedules:
            # Sort by time (most recent first) and return the latest past schedule
            today_schedules.sort(key=lambda x: x[0], reverse=True)
            return today_schedules[0][1]['temperature']
        
        return None
    finally:
        conn.close()


class SchedulerService:
    """Background service that applies zone schedules based on time and day."""
    
    def __init__(self, db_path: str, apply_temperature_callback: Callable):
        """
        Initialize scheduler service.
        
        Args:
            db_path: Path to SQLite3 database
            apply_temperature_callback: Async function(zone_id, temperature) to apply temperature
        """
        self.db_path = db_path
        self.apply_temperature_callback = apply_temperature_callback
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.last_applied: Dict[int, str] = {}  # zone_id -> last applied schedule time string
        self.timezone_cache: Optional[str] = None
        self.timezone_cache_time: Optional[float] = None
        self.timezone_cache_ttl: float = 3600.0  # Cache for 1 hour
        
    def _get_timezone(self) -> Optional[str]:
        """
        Get timezone with caching.
        
        Returns cached timezone if available and not expired, otherwise fetches from database.
        """
        current_time = time.time()
        
        # Check if cache is valid
        if (self.timezone_cache is not None and 
            self.timezone_cache_time is not None and
            current_time - self.timezone_cache_time < self.timezone_cache_ttl):
            return self.timezone_cache
        
        # Fetch from database
        timezone_str = get_timezone(self.db_path)
        self.timezone_cache = timezone_str
        self.timezone_cache_time = current_time
        
        if timezone_str:
            logger.info(f"Scheduler using timezone: {timezone_str}")
        else:
            logger.info("Scheduler using UTC (no timezone configured)")
        
        return timezone_str
    
    def _get_local_now(self) -> datetime.datetime:
        """Get current local time using cached timezone."""
        timezone_str = self._get_timezone()
        
        # Get current UTC time
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        
        if timezone_str:
            try:
                # Validate timezone
                ZoneInfo(timezone_str)
                # Convert to local time
                return utc_to_local(utc_now, timezone_str)
            except Exception as e:
                logger.warning(f"Invalid timezone '{timezone_str}': {e}, using UTC")
                return utc_now
        
        # No timezone configured, use UTC
        return utc_now
    
    async def start(self):
        """Start the scheduler background task."""
        if self.running:
            logger.warning("Scheduler service already running")
            return
        
        # Initialize timezone cache
        self._get_timezone()
        
        self.running = True
        self.task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler service started")
    
    async def stop(self):
        """Stop the scheduler background task."""
        if not self.running:
            return  # Already stopped
        
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                # Wait for cancellation with timeout to avoid hanging
                await asyncio.wait_for(self.task, timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Scheduler task did not stop within timeout, continuing shutdown")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"Error while stopping scheduler task: {e}")
        logger.info("Scheduler service stopped")
    
    async def _scheduler_loop(self):
        """Main scheduler loop that checks schedules every minute."""
        try:
            while self.running:
                try:
                    await self._check_and_apply_schedules()
                    # Wait 60 seconds before next check, but check self.running periodically
                    for _ in range(60):  # Check every second for 60 seconds
                        if not self.running:
                            return  # Exit immediately when stopped
                        await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.debug("Scheduler loop cancelled")
                    return
                except Exception as e:
                    if not self.running:
                        return  # Exit if stopped during error handling
                    logger.error(f"Error in scheduler loop: {e}")
                    # Wait a bit before retrying on error, but check self.running
                    for _ in range(10):  # Check every second for 10 seconds
                        if not self.running:
                            return  # Exit immediately when stopped
                        await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug("Scheduler loop cancelled (outer)")
        finally:
            logger.debug("Scheduler loop exiting")
    
    async def _check_and_apply_schedules(self):
        """Check all active schedules and apply matching ones."""
        now = self._get_local_now()
        current_day = now.weekday()  # 0=Monday, 6=Sunday
        current_time_rounded = round_to_5_minutes(now)
        current_time_str = current_time_rounded.strftime('%H:%M')
        
        # Get all enabled schedules
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("""
                SELECT schedule_id, zone_id, schedule_type, days_of_week, time, temperature
                FROM zone_schedules
                WHERE enabled = 1
            """)
            
            schedules = []
            for row in cursor.fetchall():
                schedules.append({
                    'schedule_id': row[0],
                    'zone_id': row[1],
                    'schedule_type': row[2],
                    'days_of_week': row[3],
                    'time': row[4],
                    'temperature': row[5],
                    'enabled': True
                })
            
            # Check zone mode tracking for each zone
            cursor = conn.execute("""
                SELECT zone_id, current_mode, manual_override_active
                FROM zone_mode_tracking
            """)
            
            zone_modes = {}
            for row in cursor.fetchall():
                zone_modes[row[0]] = {
                    'current_mode': row[1],
                    'manual_override_active': bool(row[2])
                }
        finally:
            conn.close()
        
        # Track which zones had schedules applied at current time
        zones_applied_at_current_time = set()
        
        # Process each schedule - first check for schedules matching current time
        for schedule in schedules:
            # Check if we should stop processing
            if not self.running:
                return  # Exit immediately if stopped
            
            zone_id = schedule['zone_id']
            
            # Check if schedule matches current time/day
            if not matches_schedule(schedule, now, current_day):
                continue
            
            # Check zone mode - only apply if in AUTO mode (3) and no manual override
            zone_mode = zone_modes.get(zone_id, {})
            if zone_mode.get('current_mode') != 3:  # Not in AUTO mode
                continue
            
            if zone_mode.get('manual_override_active', False):
                continue  # Manual override is active, skip scheduler
            
            # Check if we already applied this schedule at this time
            schedule_key = f"{schedule['time']}_{current_day}"
            if self.last_applied.get(zone_id) == schedule_key:
                continue  # Already applied
            
            # Apply the temperature (with timeout to avoid blocking shutdown)
            try:
                logger.info(
                    f"Scheduler: Applying schedule {schedule['schedule_id']} to zone {zone_id}: "
                    f"{schedule['temperature']}°C at {schedule['time']}"
                )
                # Use timeout to prevent blocking shutdown
                await asyncio.wait_for(
                    self.apply_temperature_callback(zone_id, schedule['temperature']),
                    timeout=5.0
                )
                self.last_applied[zone_id] = schedule_key
                zones_applied_at_current_time.add(zone_id)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout applying schedule {schedule['schedule_id']} to zone {zone_id}")
            except Exception as e:
                logger.error(f"Failed to apply schedule {schedule['schedule_id']} to zone {zone_id}: {e}")
        
        # For zones in AUTO mode that didn't get a schedule applied at current time,
        # check if there's a past schedule that should be applied (e.g., after long downtime)
        for zone_id, zone_mode in zone_modes.items():
            # Check if we should stop processing
            if not self.running:
                return  # Exit immediately if stopped
            
            # Only process zones in AUTO mode with no manual override
            if zone_mode.get('current_mode') != 3:  # Not in AUTO mode
                continue
            
            if zone_mode.get('manual_override_active', False):
                continue  # Manual override is active, skip scheduler
            
            # Skip if we already applied a schedule at current time for this zone
            if zone_id in zones_applied_at_current_time:
                continue
            
            # Find the most recent past schedule for this zone that applies today
            zone_schedules = [s for s in schedules if s['zone_id'] == zone_id]
            today_schedules = []
            
            for schedule in zone_schedules:
                # Check if schedule applies to today
                schedule_type = schedule['schedule_type']
                days_of_week = schedule['days_of_week']
                
                applies_today = False
                if schedule_type == 'any_day':
                    applies_today = True
                elif schedule_type == 'week_weekends':
                    applies_today = day_matches_week_weekends(days_of_week, current_day)
                else:  # day_of_week
                    try:
                        days_list = json.loads(days_of_week)
                        applies_today = current_day in days_list
                    except (json.JSONDecodeError, TypeError):
                        applies_today = False
                
                if applies_today:
                    # Parse schedule time
                    schedule_time = parse_time(schedule['time'])
                    schedule_datetime = now.replace(hour=schedule_time[0], minute=schedule_time[1], second=0, microsecond=0)
                    
                    # Check if this schedule time has passed today (but not exactly at current time)
                    if schedule_datetime < current_time_rounded:
                        today_schedules.append((schedule_datetime, schedule))
            
            if today_schedules:
                # Sort by time (most recent first) and get the latest past schedule
                today_schedules.sort(key=lambda x: x[0], reverse=True)
                most_recent_schedule = today_schedules[0][1]
                
                # Check if we already applied this schedule recently
                # Use a more lenient key that includes the schedule time but not the exact day
                # This allows applying the same schedule if service was down for multiple days
                schedule_key = f"{most_recent_schedule['time']}_{current_day}"
                
                # Only apply if we haven't applied this schedule recently (within last 5 minutes)
                # or if the last applied schedule is different
                last_applied_key = self.last_applied.get(zone_id)
                if last_applied_key != schedule_key:
                    # Apply the most recent past schedule
                    try:
                        logger.info(
                            f"Scheduler: Applying past schedule {most_recent_schedule['schedule_id']} to zone {zone_id}: "
                            f"{most_recent_schedule['temperature']}°C (scheduled at {most_recent_schedule['time']}, "
                            f"current time {current_time_str})"
                        )
                        # Use timeout to prevent blocking shutdown
                        await asyncio.wait_for(
                            self.apply_temperature_callback(zone_id, most_recent_schedule['temperature']),
                            timeout=5.0
                        )
                        self.last_applied[zone_id] = schedule_key
                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout applying past schedule {most_recent_schedule['schedule_id']} to zone {zone_id}")
                    except Exception as e:
                        logger.error(f"Failed to apply past schedule {most_recent_schedule['schedule_id']} to zone {zone_id}: {e}")
        
        # Clean up old entries from last_applied (keep only recent ones)
        # This prevents memory growth over time
        if len(self.last_applied) > 1000:
            # Keep only the most recent 500 entries
            self.last_applied = dict(list(self.last_applied.items())[-500:])
