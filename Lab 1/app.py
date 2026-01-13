import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
import traceback
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# LOGGING SETUP
# ============================================================================

def init_logging():
    """Initialize logging to file and console"""
    log_file = 'scraper_log.txt'
    
    # Create logger
    logger = logging.getLogger('YouTubeScraper')
    logger.setLevel(logging.DEBUG)
    
    # Prevent duplicate handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = init_logging()

# ============================================================================
# ZENROWS API CALL
# ============================================================================

def call_zenrows(url, api_key, js_render=True, wait_time=5000):
    """
    Call ZenRows API to scrape the provided URL
    
    Args:
        url: Target URL to scrape
        api_key: ZenRows API key
        js_render: Whether to enable JavaScript rendering
        wait_time: Time to wait for page load in milliseconds
    
    Returns:
        tuple: (success: bool, response_text: str or None, error_message: str or None)
    """
    zenrows_url = "https://api.zenrows.com/v1/"
    
    params = {
        'apikey': api_key,
        'url': url,
    }
    
    if js_render:
        params['js_render'] = 'true'
        params['wait'] = str(wait_time)
    
    logger.info(f"Initiating ZenRows request for URL: {url}")
    logger.debug(f"Parameters: js_render={js_render}, wait={wait_time}ms")
    
    try:
        response = requests.get(zenrows_url, params=params, timeout=60)
        
        # Log response details
        logger.info(f"ZenRows API Response - Status Code: {response.status_code}")
        
        if response.status_code == 200:
            logger.info(f"Successfully retrieved content ({len(response.text)} characters)")
            return True, response.text, None
        
        # Handle specific error codes
        elif response.status_code == 401:
            error_msg = f"401 Unauthorized - Invalid API Key. Response: {response.text[:500]}"
            logger.error(error_msg)
            return False, None, error_msg
        
        elif response.status_code == 403:
            error_msg = f"403 Forbidden - Access denied. Response: {response.text[:500]}"
            logger.error(error_msg)
            return False, None, error_msg
        
        elif response.status_code == 400:
            error_msg = f"400 Bad Request - Invalid parameters. Response: {response.text[:500]}"
            logger.error(error_msg)
            return False, None, error_msg
        
        elif response.status_code == 500:
            error_msg = f"500 Internal Server Error - ZenRows API issue. Response: {response.text[:500]}"
            logger.error(error_msg)
            return False, None, error_msg
        
        else:
            error_msg = f"HTTP {response.status_code} - {response.text[:500]}"
            logger.error(error_msg)
            return False, None, error_msg
    
    except requests.exceptions.Timeout:
        error_msg = "Request timeout - ZenRows API took too long to respond"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg
    
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error - {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Request exception - {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg
    
    except Exception as e:
        error_msg = f"Unexpected error - {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg

# ============================================================================
# HTML PARSING
# ============================================================================

def convert_views_to_number(views_text):
    """
    Convert YouTube view count from abbreviated format to exact number
    Examples: "8.8K views" -> 8800, "1.2M views" -> 1200000, "523 views" -> 523
    
    Args:
        views_text: String containing view count (e.g., "8.8K views")
    
    Returns:
        int: Exact view count as integer
    """
    try:
        # Remove "views" or "view" text
        text = views_text.replace(' views', '').replace(' view', '').strip()
        
        # Check for K (thousands)
        if 'K' in text:
            number = float(text.replace('K', ''))
            return int(number * 1000)
        
        # Check for M (millions)
        elif 'M' in text:
            number = float(text.replace('M', ''))
            return int(number * 1000000)
        
        # Check for B (billions)
        elif 'B' in text:
            number = float(text.replace('B', ''))
            return int(number * 1000000000)
        
        # Regular number with commas
        else:
            return int(text.replace(',', ''))
    
    except Exception as e:
        logger.warning(f"Could not convert views '{views_text}': {str(e)}")
        return 0

def parse_html(html_content):
    """
    Parse YouTube channel videos page HTML
    
    Args:
        html_content: Raw HTML string
    
    Returns:
        tuple: (success: bool, dataframe: pd.DataFrame or None, error_message: str or None)
    """
    logger.info("Starting HTML parsing")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        videos = []
        
        # Multiple selector strategies for video containers
        selectors = [
            'ytd-grid-video-renderer',
            'ytd-rich-item-renderer',
            'div#dismissible'
        ]
        
        video_elements = []
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                logger.info(f"Found {len(elements)} video elements using selector: {selector}")
                video_elements = elements
                break
        
        if not video_elements:
            error_msg = "No video elements found. Page structure may have changed or content didn't load."
            logger.error(error_msg)
            logger.debug(f"HTML preview (first 1000 chars): {html_content[:1000]}")
            return False, None, error_msg
        
        # Parse each video
        for idx, element in enumerate(video_elements):
            try:
                # Extract title - try multiple selectors
                title = None
                title_selectors = [
                    '#video-title',
                    'a#video-title-link',
                    'a[title]',
                    'h3 a',
                    'yt-formatted-string#video-title'
                ]
                
                for selector in title_selectors:
                    title_elem = element.select_one(selector)
                    if title_elem:
                        title = title_elem.get('title') or title_elem.get_text(strip=True)
                        if title:
                            break
                
                # Extract URL
                url = None
                url_elem = element.select_one('a#video-title-link, a#video-title')
                if url_elem and url_elem.get('href'):
                    href = url_elem['href']
                    url = f"https://www.youtube.com{href}" if href.startswith('/') else href
                
                # Extract metadata (views and time)
                views = 0
                time_posted = "N/A"
                
                metadata_selectors = [
                    '#metadata-line span',
                    'div#metadata-line span',
                    'ytd-video-meta-block span'
                ]
                
                for selector in metadata_selectors:
                    metadata_spans = element.select(selector)
                    if len(metadata_spans) >= 2:
                        views_text = metadata_spans[0].get_text(strip=True)
                        # Convert abbreviated views (8.8K, 1.2M) to exact numbers
                        views = convert_views_to_number(views_text)
                        time_posted = metadata_spans[1].get_text(strip=True)
                        break
                    elif len(metadata_spans) == 1:
                        views_text = metadata_spans[0].get_text(strip=True)
                        views = convert_views_to_number(views_text)
                
                # Only add if we got at least title and URL
                if title and url:
                    videos.append({
                        'Title': title,
                        'URL': url,
                        'Views': views,
                        'Posted': time_posted,
                        'Scraped At': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    logger.debug(f"Parsed video {idx + 1}: {title[:50]}...")
                
            except Exception as e:
                logger.warning(f"Failed to parse video element {idx + 1}: {str(e)}")
                continue
        
        if not videos:
            error_msg = "No videos were successfully parsed. Check if the page loaded correctly."
            logger.error(error_msg)
            return False, None, error_msg
        
        df = pd.DataFrame(videos)
        logger.info(f"Successfully parsed {len(df)} videos")
        return True, df, None
    
    except Exception as e:
        error_msg = f"Parsing failed - {str(e)}"
        logger.error(error_msg)
        logger.debug(traceback.format_exc())
        return False, None, error_msg

# ============================================================================
# STREAMLIT UI
# ============================================================================

def main():
    st.set_page_config(
        page_title="YouTube Channel Scraper",
        page_icon="🎥",
        layout="wide"
    )
    
    st.title("🎥 YouTube Channel Video Scraper")
    st.markdown("*Powered by ZenRows API*")
    
    # Load API key from environment
    api_key = os.getenv('ZENROWS_API_KEY')
    
    if not api_key:
        st.error("❌ ZenRows API key not found!")
        st.info("Please create a `.env` file in the project directory with: `ZENROWS_API_KEY=your_api_key_here`")
        st.stop()
    else:
        st.success(f"✅ API Key loaded (ends with ...{api_key[-8:]})")
    
    # Initialize session state
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = None
    
    # Sidebar Configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        js_render = st.checkbox(
            "Enable JS Rendering",
            value=True,
            help="Required for dynamically loaded YouTube content"
        )
        
        wait_time = st.slider(
            "Wait Time (ms)",
            min_value=1000,
            max_value=10000,
            value=5000,
            step=1000,
            help="Time to wait for page to load"
        )
        
        st.divider()
        st.caption("📝 All events are logged to `scraper_log.txt`")
    
    # Main Content Area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        channel_url = st.text_input(
            "YouTube Channel URL",
            placeholder="https://www.youtube.com/@channelname/videos",
            help="Enter the channel's /videos page URL"
        )
    
    with col2:
        st.write("")
        st.write("")
        scrape_button = st.button("🚀 Start Scraping", type="primary", use_container_width=True)
    
    # Process Scraping
    if scrape_button:
        if not channel_url:
            st.error("❌ Please enter a YouTube channel URL")
            logger.warning("Scrape attempt without URL")
        else:
            # Ensure URL points to /videos endpoint
            if '/videos' not in channel_url:
                if channel_url.endswith('/'):
                    channel_url = channel_url + 'videos'
                else:
                    channel_url = channel_url + '/videos'
                st.info(f"ℹ️ Automatically appended /videos to URL: {channel_url}")
            
            logger.info("="*80)
            logger.info("NEW SCRAPE SESSION STARTED")
            logger.info("="*80)
            
            with st.spinner("🔄 Scraping channel data..."):
                # Call ZenRows API
                success, html_content, error = call_zenrows(
                    url=channel_url,
                    api_key=api_key,
                    js_render=js_render,
                    wait_time=wait_time
                )
                
                if not success:
                    st.error(f"❌ API Request Failed: {error}")
                    logger.error(f"Scrape session failed at API call stage")
                else:
                    # Parse HTML
                    success, df, error = parse_html(html_content)
                    
                    if not success:
                        st.error(f"❌ Parsing Failed: {error}")
                        logger.error(f"Scrape session failed at parsing stage")
                    else:
                        st.session_state.scraped_data = df
                        st.success(f"✅ Successfully scraped {len(df)} videos!")
                        logger.info(f"Scrape session completed successfully - {len(df)} videos")
    
    # Display Results
    if st.session_state.scraped_data is not None:
        st.divider()
        st.subheader("📊 Scraped Videos")
        
        # Display DataFrame
        st.dataframe(
            st.session_state.scraped_data,
            use_container_width=True,
            height=400
        )
        
        # Download Button
        csv = st.session_state.scraped_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download CSV",
            data=csv,
            file_name=f"youtube_videos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
        
        # Statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Videos", len(st.session_state.scraped_data))
        with col2:
            st.metric("Columns", len(st.session_state.scraped_data.columns))
        with col3:
            st.metric("Session", datetime.now().strftime('%H:%M:%S'))
    
    # Developer Logs Section
    st.divider()
    with st.expander("🔧 Developer Logs", expanded=False):
        if os.path.exists('scraper_log.txt'):
            try:
                with open('scraper_log.txt', 'r', encoding='utf-8') as f:
                    log_content = f.read()
                    if log_content:
                        # Show last 100 lines
                        lines = log_content.split('\n')
                        display_lines = lines[-100:] if len(lines) > 100 else lines
                        st.code('\n'.join(display_lines), language='log')
                    else:
                        st.info("Log file is empty")
            except Exception as e:
                st.error(f"Could not read log file: {str(e)}")
        else:
            st.info("No log file found yet. Start a scraping session to generate logs.")
        
        if st.button("🗑️ Clear Logs"):
            try:
                with open('scraper_log.txt', 'w') as f:
                    f.write('')
                st.success("Logs cleared!")
                st.rerun()
            except Exception as e:
                st.error(f"Could not clear logs: {str(e)}")

if __name__ == "__main__":
    main()