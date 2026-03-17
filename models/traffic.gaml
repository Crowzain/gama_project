/**
* Name: traffic
* Based on the minhduc0711 model. 
* Author: minhduc0711, pierrebeauvain
* Tags: 
*/



model traffic

global {
	string CAR <- "car";
	string MOTO <- "motorbike";
	string OUT <- "outArea";	
	graph road_network;
	float lane_width <- 1.0;
}


species intersection schedules: [] skills: [intersection_skill] {
	
}

species road  skills: [road_skill]{
	string type;
	bool oneway;
	bool s1_closed;
	bool s2_closed;
	
	rgb color <- #grey ;
	int num_lanes <- 4;
	bool closed;
	float capacity ;
	int nb_vehicles <- length(all_agents) update: length(all_agents);
	float speed_coeff <- 1.0 min: 0.1 update: 1.0 - (nb_vehicles/ capacity);
	init {
		 capacity <- 1 + (num_lanes * shape.perimeter/3);
	}
	aspect base{
		draw self.shape + 4 color: color;
	}
}


species car parent: vehicle {
	string type <- CAR;
	float vehicle_length <- 4.5 #m;
	int num_lanes_occupied <-2;
	float max_speed <-rnd(50,70) #km / #h;
		
}

species motorbike parent: vehicle {
	string type <- MOTO;
	float vehicle_length <- 2.8 #m;
	int num_lanes_occupied <-1;
	float max_speed <-rnd(40,50) #km / #h;
}

species vehicle skills:[driving] {
	string type;
	intersection target;
	point shift_pt <- location ;	
	bool at_home <- true;
	init {
		
		proba_respect_priorities <- 0.0;
		proba_respect_stops <- [1.0];
		proba_use_linked_road <- 0.0;

		lane_change_limit <- 2;
		linked_lane_limit <- 0; 
		location <- one_of(intersection).location;
	}

	action select_target_path {
		target <- one_of(intersection);
		location <- (intersection closest_to self).location;
		do compute_path graph: road_network target: intersection closest_to target; 
	}
	
	reflex choose_path when: final_target = nil  {
		do select_target_path;
	}
	
	reflex move when: final_target != nil {
		do drive;
		if (final_target = nil) {
			do unregister;
			at_home <- true;
			location <- target.location;
		} else {
			shift_pt <- compute_position();
		}
		
	}
	
	
	point compute_position {
		// Shifts the position of the vehicle perpendicularly to the road,
		// in order to visualize different lanes
		if (current_road != nil) {
			float dist <- (road(current_road).num_lanes - mean(range(num_lanes_occupied - 1)) - 0.5) * lane_width;
			if violating_oneway {
				dist <- -dist;
			}
		 	
			return location + {cos(heading + 90) * dist, sin(heading + 90) * dist};
		} else {
			return {0, 0};
		}
	}	
	
}

species stop {
	rgb color <- #yellow const:true;
	string stop_id;
	bool activated<-false;
	intersection closest_intersection;
	action initialize{
		closest_intersection <- intersection closest_to self;
	}
	
	aspect base {
		if activated {
			draw circle(10) color: color border: #black;	
		}
	}
}

