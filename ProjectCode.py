import rospy
import numpy as np
import subprocess
from gazebo_msgs.srv import SetModelState
from gazebo_msgs.msg import ModelState
from sensor_msgs.msg import LaserScan
from nav_msgs.msg import Odometry

class MultiRobotEnv:
    def __init__(self, num_robots=4, num_goals=4):
        self.num_robots = num_robots
        self.num_goals = num_goals
        self.robot_positions = np.zeros((num_robots, 3))
        self.robot_laser_data = np.zeros((num_robots, 360))
        self.goals = np.zeros((num_goals, 3))
        self.timestep = 0
        self.last_linear_velocities = np.zeros((num_robots,))
        self.last_angular_velocities = np.zeros((num_robots,))

        # Launch the Gazebo simulation
        subprocess.Popen(["roslaunch", "Hoggaan.launch"])
        
        # Wait for the Gazebo service to be available
        rospy.wait_for_service('/gazebo/set_model_state')
        self.set_model_state = rospy.ServiceProxy('/gazebo/set_model_state', SetModelState)
        
        # Set the initial positions of the robots in the Gazebo simulation
        for i in range(self.num_robots):
            model_state = ModelState()
            model_state.model_name = "robot_{}".format(i)
            model_state.pose.position.x = self.robot_positions[i][0]
            model_state.pose.position.y = self.robot_positions[i][1]
            model_state.pose.position.z = self.robot_positions[i][2]
            self.set_model_state(model_state)
        
        # Subscribe to the laser and odometry topics for each robot
        self.laser_subs = []
        self.odom_subs = []
        for i in range(self.num_robots):
            laser_sub = rospy.Subscriber("/robot_{}/scan".format(i), LaserScan, self.laser_callback, i)
            odom_sub = rospy.Subscriber("/robot_{}/odom".format(i), Odometry, self.odom_callback, i)
            #cmd_vel_pup = rospy.Publisher("/robot_{}/cmd_vel".format(i), Odometry, self.odom_callback, i)
            self.laser_subs.append(laser_sub)
            self.odom_subs.append(odom_sub)

    def laser_callback(self, msg, robot_index):
        """Callback method for processing laser scan data for a specific robot."""
        self.robot_laser_data[robot_index] = msg.ranges

    def odom_callback(self, msg, robot_index):
        """Callback method for processing odometry data for a specific robot."""
        self.robot_positions[robot_index][0] = msg.pose.pose.position.x
        self.robot_positions[robot_index][1] = msg.pose.pose.position.y
        self.robot_positions[robot_index][2] = msg.pose.pose.position.z
        self.last_linear_velocities[robot_index] = msg.twist.twist.linear.x
        self.last_angular_velocities[robot_index] = msg.twist.twist.angular.z
    
    def step(self, action):
        for i in range(self.num_robots):
            pub = rospy.Publisher("/robot_{}/cmd_vel".format(i), Twist, queue_size=10)
            twist = Twist()
            twist.linear.x = action[i][0]
            twist.angular.z = action[i][1]
            pub.publish(twist)
        self.calculate_observations(num_robots)
        reward = self.calculate_reward()
        done = self.check_done()
        return self.observations, reward, done, {}

    def reset(self):
        for i in range(self.num_robots):
            self.model_states[i].pose.position.x = 0.0
            self.model_states[i].pose.position.y = 0.0
            self.model_states[i].pose.position.z = 0.0
        self.start_time = rospy.Time.now()
        self.calculate_observations()
        return self.observations

    def calculate_reward(self):
        """Compute the reward for the current timestep of the simulation."""
        reward = 0
        
        # Check for collisions between robots
        for i in range(self.num_robots):
            for j in range(i + 1, self.num_robots):
                # If the distance between the two robots is less than a threshold, penalize
                if np.linalg.norm(self.robot_positions[i] - self.robot_positions[j]) < self.collision_threshold:
                    reward -= 1
        
        # Check for collisions between robots and obstacles
        for i in range(self.num_robots):
            # For each laser range reading for the current robot
            for j, range_reading in enumerate(self.robot_laser_data[i]):
                # If the range reading is less than a certain threshold, consider it a collision
                if range_reading < COLLISION_THRESHOLD:
                    # Add a negative reward for the collision
                    reward -= COLLISION_PENALTY
                
        # Check if any two robots have reached the same goal
        unique_goals = np.unique(robot_goals)
        if len(unique_goals) != self.num_robots:
            # If the number of unique goals is not equal to the number of robots,
            # then at least two robots have reached the same goal
            reward -= SAME_GOAL_PENALTY
            
        return reward

    def calculate_observation(self):
        observation_space = []
        for robot in self.num_robots:
            robot_observation = []
            
            # calculate the relative positions of goals in the focal robot's polar coordinates
            relative_positions = np.zeros((3, len(self.num_goals), 2))
            for i, goal in enumerate(self.num_goals):
                dx = goal[0] - robot.x
                dy = goal[1] - robot.y
                relative_positions[:, i, 0] = np.sqrt(dx**2 + dy**2)
                relative_positions[:, i, 1] = np.arctan2(dy, dx) - robot.theta
            robot_observation.append(relative_positions)
            
            # calculate the relative positions of the goals in the other robots' polar coordinates
            other_robots_relative_positions = np.zeros((len(self.robots) - 1, 3, len(self.goals), 2))
            for j, other_robot in enumerate(self.num_robots):
                if robot == other_robot:
                    continue
                for i, goal in enumerate(self.goals):
                    dx = goal[0] - other_robot.x
                    dy = goal[1] - other_robot.y
                    other_robots_relative_positions[j, :, i, 0] = np.sqrt(dx**2 + dy**2)
                    other_robots_relative_positions[j, :, i, 1] = np.arctan2(dy, dx) - other_robot.theta
            robot_observation.append(other_robots_relative_positions)
            
            # calculate the 360 degree laser scanner data of the focal robot
            laser_scanner = np.zeros((2, 360))
            for i, angle in enumerate(np.linspace(-np.pi, np.pi, 360)):
                laser_scanner[0, i] = np.cos(angle)
                laser_scanner[1, i] = np.sin(angle)
            laser_scanner *= self.laser_range
            laser_scanner += np.array([robot.x, robot.y])
            laser_scanner = np.minimum(laser_scanner, env.obstacle_mask)
            laser_scanner = np.linalg.norm(laser_scanner, axis=0)
            robot_observation.append(laser_scanner)
            
            # calculate the time spent since the focal robot started moving and the previous action
            time_spent = robot.time - robot.last_time
            previous_action = np.array([robot.last_linear_velocity, robot.last_angular_velocity])
            robot_observation.append(np.concatenate((time_spent, previous_action)))
            
            observation_space.append(robot_observation)
        return observation_space
