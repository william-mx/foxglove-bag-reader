# Foxglove Bagfile Reader for Colab

This is a lightweight Python utility for working with **ROS bagfiles stored on Foxglove Data Platform**.  
It allows you to connect with your Foxglove account, browse recordings, parse topics into pandas DataFrames,  
extract images, export them for further analysis, and also access **events** from your recordings — all directly in Google Colab or Jupyter.

---

### Quickstart

See the provided Jupyter notebook for a complete demo.  
In general, the workflow looks like this:

1. **Install the package**
 ```python
%%capture
!pip install git+https://github.com/william-mx/foxglove-bag-reader.git
```

2. **Connect with your Foxglove API key**

 ```python
 from google.colab import userdata
 API_KEY = userdata.get("FOXGLOVE_KEY")  # store your key in Colab userdata

 from foxglove_bag_reader import BagfileReader
 r = BagfileReader(API_KEY)
 ```

3. **List available recordings**

 ```python
 r.print_recordings()
 ```

4. **Example: work with images**

 ```python
 # Get images from a camera topic
 images = r.get_all_images('/camera/color/image_jpeg')

 # Export them to a directory
 export_dir = project_dir / 'export'
 r.export_images('/camera/color/image_jpeg', export_dir)
 ```

5. **Example: work with events**

 ```python
  # Open the specific bagfile recording by name
  r.get_recording_by_name('your_recording')

  # Load steering command messages (AckermannDrive) and camera frames into DataFrames
  df_ackermann = r.parse_topic('/rc/ackermann_cmd')
  df_camera = r.parse_topic('/camera/camera/color/image_raw')

  # Synchronize image and steering angle data based on timestamps
  df_synced = r.sync_dataframes(df_camera, ack=df_ackermann)

  # Extract all raw RGB images from the camera topic
  images = r.get_all_images("/camera/camera/color/image_raw")

  # Load events that were manually defined in Foxglove
  events = r.get_events()

  # Match each event to the corresponding image indices in the DataFrame
  events = r.find_indices_within_events(df_synced, events)

  snippets = [
      {
      'type': event['metadata']['your_key'],
      'images': im_processed[event['indices']],
      'angles': df_synced['steering_angle_ack'][event['indices']]
      } 
      for event in events]
   ```

Each event contains:

* `metadata`: event description
* `start`: event start timestamp (ns)
* `end`: event end timestamp (ns)

