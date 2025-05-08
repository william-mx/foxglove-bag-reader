from foxglove_data_platform.client import Client
import pandas as pd
import numpy as np
import cv2
import struct
from tqdm import tqdm
import os

class BagfileReader():
  def __init__(self, api_key):

    self.client = Client(token=api_key)

    self.call_fun_by_message = {
        'ackermann_msgs/AckermannDriveStamped': self.parse_ackermann_drive_stamped,
        'sensor_msgs/CompressedImage': self.parse_image,
        'sensor_msgs/Image': self.parse_image,
        'sensor_msgs/Imu': self.parse_imu,
        'sensor_msgs/LaserScan': self.parse_scan,
        'sensor_msgs/PointCloud2': self.parse_pointcloud,
        'std_msgs/Int16MultiArray': self.parse_multiarray,
        'sensor_msgs/MagneticField': self.parse_magneto,
        'sensor_msgs/Joy': self.parse_joy,
        'std_msgs/Float64': self.parse_float
    }

    self.recordings = {
      "bshaped_track_all": "2y57fL95RUnUG5MW",
      "bshaped_track_depth": "rec_0dY0zYREG8TYFbTY",
      "bshaped_track_following_left": "rec_0dX26NC9WkjKqTyi",
      "bshaped_track_following_right": "rec_0dX27U8jsoFpbA2u",
      "bshaped_track_odometry": "rec_0dY0ylXuthVmxy44",
      "circle_drive_fixed_speed_left": "rec_0dWf0XUVbrIrRtgL",
      "cones_lidar_slam": "rec_0dY0z7WHao9tYAZY",
      "hallway_lidar_slam": "rec_0dY106wizvoNRenI",
      "imu_speed_stairs": "rec_0dWf0csWqOJcTgWT",
      "imu_static": "rec_0dWf1wPygPWHB8ne",
      "ir_lidar_calibration": "rec_0dY10RmyGbqo3RYh",
      "lidar_erpm_speed_calibration": "rec_0dWeOf9soScOfuPB",
      "lidar_intensity_chessboard": "rec_0dY11K7XVJTQjkJp",
      "line_follow_ir": "rec_0dXdQXXtgpulyhFq",
      "line_follow_rgb": "rec_0dXdQEjD9xusMopd",
      "parking_spot_scan_boxes": "rec_0dXGZgmNC3UmsFgo",
      "parking_spot_scan_cars": "rec_0dXGZnMRxqFbrsBP",
      "realsense_ir_projector": "rec_0dY12VVqMSqqYCLi",
      "realsense_stereo_pair": "rec_0dY13AdIDwYxmq5l",
      "rgb_lidar_calibration": "rec_0dY13Iyy8JnrqtQZ",
      "straight_line_odometry_fixed": "rec_0dWf22Qu0x4iOBgs",
      "straight_line_odometry_variable": "rec_0dWf25K94kU3b079",
      "calpoly_line_follor_rgb": "rec_0dc1TgAUN7TKeVi9",
      "stop_sign_detection": "rec_0ddpAqCTWG1aGR9l",
      "all_signs": "rec_0ddyVecQl0hEpFBu",
      "pursuit_race": "rec_0ddybX0NuSnkO01W"
    }

  def print_recordings(self):
      print("Available Recordings:\n")
      for name, rec_id in self.recordings.items():
          print(f"{name.ljust(40)} → {rec_id}")


  def get_recording_by_name(self, name):
      if name not in self.recordings:
          raise ValueError(f"Recording name '{name}' not found in available recordings.")
      
      recording_id = self.recordings[name]
      return self.get_recording_by_id(recording_id)

    
  def get_recording_by_id(self, recording_id):

    rec = [r for r in  self.client.get_recordings() if r["id"] == recording_id][0]

    if len(rec) < 1:
      raise ValueError(f"Recording with id {recording_id} not found.")

    self.device_id = rec['device']['id']
    self.start = rec['start']
    self.end = rec['end']


    topics = self.client.get_topics(device_id=self.device_id, start=self.start, end=self.end)
    df = pd.DataFrame(topics)

    self.mapping = dict(zip(df['topic'], df['schema_name']))
    self._info_df = df

  def info(self):
    return self._info_df

  def _get_timestamp(self, message):

    # Extract time from the message header
    stamp = message.header.stamp
    if hasattr(stamp, 'secs'): # ros1
        secs = stamp.secs
        nsecs = stamp.nsecs
    elif hasattr(stamp, 'sec'): # ros2
        secs = stamp.sec
        nsecs = stamp.nanosec
    else:
        raise AttributeError("No valid timestamp attributes found in message.header.stamp")

    timestamp_ns = int(secs * 1e9 + nsecs)  # Convert to nanoseconds
    return timestamp_ns

  def _get_timestamp_from_mcap(self, record):
      # Extract the publish_time directly from the record
      timestamp_ns = record.publish_time
      return timestamp_ns

  @staticmethod
  def decode_ros_image(ros_image):
      """
      Decodes a raw ROS sensor_msgs/Image message into a NumPy image.

      Args:
      ros_image (sensor_msgs.msg.Image): The ROS Image message.

      Returns:
      numpy.ndarray: The decoded image in OpenCV BGR format.
      """
      # Convert raw data to a NumPy array
      np_arr = np.frombuffer(ros_image.data, dtype=np.uint8)

      # Reshape the array to match the image dimensions
      img = np_arr.reshape((ros_image.height, ros_image.width, 3))  # 3 channels for RGB

      # Convert RGB to OpenCV's BGR format for compatibility with OpenCV functions
      img_bgr = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

      return img_bgr, None

  @staticmethod
  def decode_jpeg(data):
      """
      Extract JPEG data from a custom format and decode it into an image.

      This function handles a custom data format where JPEG data is preceded by a metadata header.
      It extracts the JPEG data, decodes it into an image, and returns both the image and metadata.

      Args:
      data (bytes): Raw data containing both custom header and JPEG data.

      Returns:
      tuple: (decoded_image, metadata)
          decoded_image (numpy.ndarray): The decoded image as a NumPy array, or None if decoding fails.
          metadata (dict): A dictionary containing extracted metadata.

      Raises:
      ValueError: If the JPEG start marker is not found in the data.
      """
      # Find the start of the JPEG data (FFD8 marker)
      # The FFD8 marker, represented as b'\xFF\xD8' in bytes, indicates the Start of Image (SOI) in JPEG format.
      # This marker is always present at the beginning of a standard JPEG file.
      # The `find()` method searches for the first occurrence of this byte sequence in the data.
      # It returns the index where the sequence starts, or -1 if not found.
      # This approach allows us to handle cases where the JPEG data is preceded by custom metadata or headers.
      jpeg_start = data.find(b'\xFF\xD8')
      if jpeg_start == -1:
          raise ValueError("JPEG start marker (FFD8) not found. The data may not contain a valid JPEG image.")

      # Extract metadata from the custom header
      custom_header = data[:jpeg_start]
      metadata = {
          'header_size': len(custom_header),
          'jpeg_size': struct.unpack('>I', custom_header[16:20])[0],  # Unpack 4 bytes as big-endian unsigned int
          'format': custom_header[20:24].decode('ascii')  # Decode 4 bytes as ASCII
      }

      # Extract the JPEG data
      jpeg_data = data[jpeg_start:]

      # Decode the JPEG data into an image
      np_arr = np.frombuffer(jpeg_data, np.uint8)
      decoded_image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

      return decoded_image, metadata

  def export_images(self, topic, data_dir):
      if not os.path.exists(data_dir):
          raise FileNotFoundError(f"Directory {data_dir} does not exist.")

      export_dir = os.path.join(data_dir, self.recording_id)

      if not os.path.exists(export_dir):
          os.makedirs(export_dir)
          print(f"Saving images to the directory: {export_dir}...")

      # Create a progress bar with tqdm
      record = self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic])
      total_messages = len(record)

      for i, (topic, record, message) in enumerate(tqdm(record, total=total_messages, desc="Exporting images")):
          path = os.path.join(export_dir, f'frame_{i:04d}.jpg')
          image, _ = self.decode_jpeg(record.data)
          cv2.imwrite(path, image)

  def get_all_images(self, topic):
    data = []

    if topic not in self.mapping.keys():
      raise ValueError("Topic not existant in recording.")

    msg_type = self.mapping[topic]

    for (topic, record, message) in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):
        image, _ = self.decode_jpeg(record.data) if msg_type == 'sensor_msgs/CompressedImage' else self.decode_ros_image(message)
        data.append(image)

    return data

  def get_image_by_index(self, topic, index):
    for i, (topic, record, message) in enumerate(self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic])):
      if i == index:
        image, _ = self.decode_jpeg(record.data)
        return image

  def parse_image(self, topic):
    data = []
    for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):
      timestamp_ns = self._get_timestamp(message)


      # Append the data to the list
      data.append({
          'timestamp_ns': timestamp_ns
      })

    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data)

    return df

  def parse_ackermann_drive_stamped(self, topic):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp(message)

        # Extract steering angle and speed from the message
        steering_angle = message.drive.steering_angle
        speed = message.drive.speed

        # Append the data to the list
        data.append({
            'timestamp_ns': timestamp_ns,
            'steering_angle': steering_angle,
            'speed': speed
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_imu(self, topic):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp(message)

        # rotational velocity rad/sec
        # x: roll rate (rotation around x-axis)
        # y: pitch rate (rotation around y-axis)
        # z: yaw rate (rotation around z-axis)
        rot_x = message.angular_velocity.x
        rot_y = message.angular_velocity.y
        rot_z = message.angular_velocity.z

        accel_x = message.linear_acceleration.x
        accel_y = message.linear_acceleration.y
        accel_z = message.linear_acceleration.z

        # Append the data to the list
        data.append({
            'timestamp_ns': timestamp_ns,
            'gyro_x': rot_x,
            'gyro_y': rot_y,
            'gyro_z': rot_z,
            'accel_x': accel_x,
            'accel_y': accel_y,
            'accel_z': accel_z
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_scan(self, topic):

      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):
        pointcloud = []
        timestamp_ns = self._get_timestamp(message)

        angle_min = message.angle_min
        angle_max = message.angle_max
        angle_increment = message.angle_increment
        range_min = message.range_min
        range_max = message.range_max
        ranges = message.ranges
        intensities = message.intensities

        # Generate angles for each range measurement
        angles = np.arange(angle_min, angle_max+angle_increment, angle_increment)

        # Iterate over ranges and compute corresponding 3D points (z=0)
        for r, angle in zip(ranges, angles):
            if range_min < r < range_max:  # Filter out invalid range values
                x = r * np.cos(angle)
                y = r * np.sin(angle)
                z = 0  # Assuming 2D scan, z is zero

                pointcloud.append([x, y, z])

        # Append the data to the list
        data.append({
            'timestamp_ns': timestamp_ns,
            'ranges': ranges,
            'pointcloud': pointcloud
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_pointcloud(self, topic):
      # List to store the parsed data
      data = []

      record = self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic])
      total_messages = len(record)

      for i, (topic, record, message) in enumerate(tqdm(record, total=total_messages, desc="Exporting pointclouds")):
          pointcloud = []

          timestamp_ns = self._get_timestamp(message)

          # Access the data field from the message
          point_cloud_data = message.data
          point_step = message.point_step  # Should be 18 bytes
          num_points = message.width

          # Iterate through each point and extract x, y, z, intensity, tag, line
          for i in range(num_points):
              # Calculate the offset for the current point
              offset = i * point_step

              # Unpack the x, y, z, intensity values (4 floats = 16 bytes)
              x, y, z, intensity = struct.unpack_from('<ffff', point_cloud_data, offset)

              # Unpack tag and line (2 uint8 = 2 bytes)
              tag, line = struct.unpack_from('<BB', point_cloud_data, offset + 16)

              # Append the point data to the list
              # You can include tag and line if needed
              pointcloud.append([x, y, z, intensity])

          data.append({
              'timestamp_ns': timestamp_ns,
              'pointcloud': np.array(pointcloud, dtype=np.float32)
          })

      df = pd.DataFrame(data)
      return df

  def parse_multiarray(self, topic):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp_from_mcap(record)

        # Extract uss measurements
        uss_values = message.data

        data.append({
            'timestamp_ns': timestamp_ns,
            'uss_values': uss_values
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_float(self, topic, name = 'value'):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp_from_mcap(record)

        data.append({
            'timestamp_ns': timestamp_ns,
            name: message.data
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_magneto(self, topic):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp(message)

        # Extract field vector in Tesla
        vec = message.magnetic_field

        data.append({
            'timestamp_ns': timestamp_ns,
            'x': vec.x,
            'y': vec.y,
            'z': vec.z
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def parse_joy(self, topic):
      # List to store the parsed data
      data = []

      # Fetch messages from the given topic
      for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):

        timestamp_ns = self._get_timestamp(message)

        data.append({
            'timestamp_ns': timestamp_ns,
            'axes': message.axes,
            'buttons': message.buttons
        })

      # Convert the list of dictionaries to a Pandas DataFrame
      df = pd.DataFrame(data)

      return df

  def get_sample_message(self, topic):
    for topic, record, message in self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=[topic]):
      return message

  def get_record(self, topics: list):
    for topic in topics:
      if topic not in self.mapping.keys():
        raise ValueError(f"{topic} does not exist in the recording!")

    return self.client.get_messages(device_id=self.device_id, start=self.start, end=self.end, topics=topics)

  def parse_topic(self, topic):

    if topic not in self.mapping.keys():
      raise ValueError("Topic not existant in recording.")

    msg_type = self.mapping[topic]

    msg_type = msg_type.replace("/msg/", "/") # sensor_msgs/msg/LaserScan → sensor_msgs/LaserScan

    return self.call_fun_by_message[msg_type](topic)


  @staticmethod
  def sync_dataframes(base_df, **dataframes):
      """
      Synchronizes multiple secondary dataframes with a base dataframe based on their timestamps.
      For each row in base_df, finds the closest timestamp in each secondary dataframe and adds the
      corresponding data as new columns. The output always has the same number of rows as base_df.

      Args:
          base_df (pd.DataFrame): The base dataframe containing the primary timestamps.
          **dataframes: Arbitrary keyword arguments where each key is a unique identifier and each value
                        is a secondary dataframe to sync with base_df.

      Returns:
          pd.DataFrame: A new dataframe with the same number of rows as base_df, augmented with data from
                        each secondary dataframe.

      Raises:
          ValueError: If any dataframe (base or secondary) is missing the 'timestamp_ns' column.
      """
      # Ensure the base DataFrame has a proper, sequential index
      base_df = base_df.reset_index(drop=True)

      # Check if the base dataframe has the 'timestamp_ns' column
      if 'timestamp_ns' not in base_df.columns:
          raise ValueError("The base dataframe must have a 'timestamp_ns' column")

      # Copy the base DataFrame to start building the synced DataFrame
      synced_df = base_df.copy()

      # For each secondary dataframe passed as a keyword argument
      for key, df_secondary in dataframes.items():
          # Reset the index of the secondary dataframe too (optional, for safety)
          df_secondary = df_secondary.reset_index(drop=True)

          # Check for required timestamp column
          if 'timestamp_ns' not in df_secondary.columns:
              raise ValueError(f"The dataframe for key '{key}' is missing the 'timestamp_ns' column")

          # Prepare lists to hold the synchronized data for the secondary dataframe
          time_errors = []
          secondary_timestamps = []
          secondary_data = {col: [] for col in df_secondary.columns if col != 'timestamp_ns'}

          # Iterate over each base timestamp
          for base_time in base_df['timestamp_ns']:
              # Find the index of the closest timestamp in the secondary dataframe
              closest_secondary_idx = (df_secondary['timestamp_ns'] - base_time).abs().idxmin()
              closest_secondary_row = df_secondary.loc[closest_secondary_idx]
              secondary_time = closest_secondary_row['timestamp_ns']

              # Calculate time error (in milliseconds)
              time_error_ms = abs(base_time - secondary_time) / 1e6  # ns -> ms

              time_errors.append(time_error_ms)
              secondary_timestamps.append(secondary_time)

              # For every other column in the secondary dataframe, store the corresponding value
              for col in secondary_data.keys():
                  secondary_data[col].append(closest_secondary_row[col])

          # Add the calculated values as new columns in synced_df with unique names based on the key
          synced_df[f'time_error_ms_{key}'] = time_errors
          synced_df[f'timestamp_ns_{key}'] = secondary_timestamps
          for col, values in secondary_data.items():
              synced_df[f'{col}_{key}'] = values

      return synced_df


