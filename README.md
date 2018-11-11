# VehicleTelematics

Stream processing of simulated on-vehicle sensors data using Apache Flink.

Each vehicle reports a position event every 30 seconds with the
following format: (Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)

Time will be a timestamp (integer) in seconds identifying the time at which the position event was emitted.

VID: Integer that identifies the vehicle,
Spd (0 - 100): Integer that represents the speed mph (miles per hour) of the vehicle,
XWay (0 . . .L−1): Identifies the highway from which the position report is emitted
Lane (0 . . . 4): Identifies the lane of the highway from which the position report is emitted (0 if it is an
entrance ramp (ENTRY), 1 − 3 if it is a travel lane (TRAVEL) and 4 if it is an exit ramp (EXIT)).
Dir (0 . . . 1): Indicates the direction (0 for Eastbound and 1 for Westbound) the vehicle is traveling.
Seg (0 . . . 99): Identifies the segment from which the position report is emitted
Pos (0 . . . 527999): Identifies the horizontal position of the vehicle as the number of meters from the
westernmost point on the highway (i.e., Pos = x)

Functionalities implemented:

Speed Radar: detects cars that overcome the speed limit of 90 mph.

Average Speed Control: detects cars with an average speed higher than 60 mph between
segments 52 and 56 (both included) in both directions. If a car sends several reports on
segments 52 or 56, the ones taken for the average speed are the ones that cover a longer
distance.

Accident Reporter: detects stopped vehicles on any segment. A vehicle is stopped when it
reports at least 4 consecutive events from the same position.
