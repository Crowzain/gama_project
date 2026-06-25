/**
* Name: projectZZ3
* Based on the internal empty template. 
* Author: pierrebeauvain
* Tags: 
*/


model projectZZ3

import "traffic.gaml"

global {
	
	//files variables
	file shape_file_buildings <- shape_file("../includes/reduced_data/reduced_buildings.shp") const:true;
	file shape_file_roads <- shape_file("../includes/reduced_data/reduced_roads.shp") const:true;
	file file_stops<-shape_file("../includes/reduced_data/reduced_stops.shp") const:true;
	file file_lines<-csv_file("../includes/reduced_data/lines.txt", ",", string, true) const:true;
	
	geometry shape <- envelope(shape_file_roads) const:true;
	
	//parameters
	int initial_passengers_nb <- 10 const:true;
	int passengers_nb <- initial_passengers_nb;
	int min_capacity <- 5 const:true;
	int max_capacity <- 30 const:true;
	float eps <- 50.0#m const:true;
	float distance_from_building_tolerance <- 200.0#m const:true;
	int int_seed;
	bool verbose_mode <- false const:true;
	bool batch_mode <- false const:false;
	float T_max <- 6 #h const:true;
	float spawn_frequency<-6.5#mn;
	bool is_output_flag <- false;
	
	int motorbikes_nb <- 100;
	int cars_nb <- 200;
	int buses_nb_per_line <- 3;
	
	list<float> waiting_time_list <-[];
	list<float> time_to_reach_target_list <-[];
	list<float> passengers_ratio_list <-[];
	

	//graphs	
	graph network_skeleton <- as_edge_graph(shape_file_roads);
	graph road_graph;
	graph<stop,stop> bus_graph <-graph([]);
	
	//index map
	map<string,stop> stop_index <- [];
	
	reflex stop_simulation when: time >= T_max {
		if batch_mode{
			if not is_output_flag{
				do write_stats;
				is_output_flag <- true;	
			}
		}
		else{
			do pause;
		}
	}
	
	reflex make_stops_spawn_passengers when: every(spawn_frequency) {
		ask stop{
			do call_passenger_factory;	
		}
	}
	
	action write_stats{
		
		waiting_time_list <- waiting_time_list sort_by each;
		time_to_reach_target_list <- time_to_reach_target_list sort_by each;
		
		float Q25 <- quantile(waiting_time_list, 0.25);
		float Q50 <- median(waiting_time_list);
		float Q75 <- quantile(waiting_time_list, 0.75);
		
		save [seed, Q25, Q50, Q75]
		to: "../results/save_wt"+"f_"+ spawn_frequency+ ".csv" format: "csv" rewrite:false;
		
		Q25 <- quantile(time_to_reach_target_list, 0.25);
		Q50 <- median(time_to_reach_target_list);
		Q75 <- quantile(time_to_reach_target_list, 0.75);
		
		save [seed, Q25, Q50, Q75]
		to: "../results/save_reach_target"+"f_"+ spawn_frequency+ ".csv" format: "csv" rewrite:false;
		
		save [seed, passengers_ratio_list]
		to: "../results/save_list"+"f_"+ spawn_frequency+ ".csv" format: "csv" rewrite:false;
	}
	
	action filter_stops{
		ask stop {
			if not activated{
				remove from:stop_index index:self.stop_id;
				do die;
			}
		}
	}
	
	action initialize_buses(int n_buses){
		stop s;
		int current_stop_indx;
		if n_buses=1{
			loop l over:busLine{
				do create_randomly_one_bus_on_line(l, n_buses);
			}
		}
		else{
			loop l over:busLine{
				do create_uniformally_n_buses_on_line(l, n_buses);
			}
		}
	}
	
	action create_randomly_one_bus_on_line(busLine l, int n_buses){
		create bus with:[line:l, color:l.color]{
			do set_random_initial_state;
			do update_next_stop;
		}
	}
	
	action create_uniformally_n_buses_on_line(busLine l, int n_buses){
		int line_length <- length(l.stops);
		int gap<-max(1, line_length-1 div n_buses);
		loop i from:0 to:line_length-1 step:gap {
			create bus with:[line:l, color:l.color]{
				do set_initial_state(i);
				do update_next_stop;
			}
		}
	}
	
	action fill_stop_index_map{
		ask stop {
    		stop_index[stop_id] <- self;
    		location <- shape_file_roads closest_to(self);
		}
	}
	
	action create_driving_graph{
		do create_roads;
		do create_intersections;
		do make_connections;
	}

	action create_roads{
		create road from: shape_file_roads with:[
			maxspeed::float(read('maxspeed'))#km/#h, oneway::bool('oneway')
		]{
			if maxspeed < 1#km/#h{
				maxspeed <- 10.0#km/#h;
			}
			if oneway = false{
				do create_opposite_direction_road(self);
			}
		}
	}

	action create_intersections{
		loop v over:network_skeleton.vertices{
			create intersection with:[location::(v as point).location];
		}
	}

	action make_connections{

		ask road{
			intersection source <- closest_to(intersection, first(shape.points));
			intersection target <- closest_to(intersection, last(shape.points));

			if oneway = true{
				intersection tmp <- source;
				source_node <- target;
				target_node <- tmp;
			}


			do connect_two_intersections(source, target);
			
		}
		map edge_weights <- road as_map (each::each.shape.perimeter);
		road_graph<-as_driving_graph(road, intersection) with_weights edge_weights;
	}
	
	/* 
	//Database settings
	map<string, string> MySQL <- [
			'host'::'localhost', 
			'dbtype'::'mysql', 
			'database'::'mysql_db', 
			'port'::'3307', 
			'user'::'root', 
			'passwd'::'Iamastrongpassword'
			]; // is it possible to access environment variable ?
	map <string, string>  SQLITE <- ['dbtype'::'sqlite', 'database'::'../includes/gama_project_sqlite.db'];
	string QUERY <- "SELECT stop_id FROM stops;";
	*/
	
	init {		
		
		step <- 4.9#s;
		rng <- "java";
		
		seed<-float(int_seed);
		if (seed != ceil(seed)){
			seed <- 1.0;
		}
		
		create building from: shape_file_buildings;
		
		/* 
		create agtDB{
			do connect params:SQLITE;
		}
		ask agtDB {
			write "Connection to SQLITE is " +  testConnection(SQLITE);
			write select(QUERY);
        }
        */
		create stop from:file_stops with:[stop_id::read ('stop_id'), location::point(read('geometry'))];
		
		do create_driving_graph;
		
		do fill_stop_index_map;

		create busLine from:file_lines with:[route_id::string(read ('name')), color::rgb([read ('r'), read ('g'), read ('b')])];
		
		do filter_stops;
		do initialize_buses(buses_nb_per_line);
		ask stop{
			do call_passenger_factory;
		}
		
		create motorbike number:motorbikes_nb;
		create car number:cars_nb;
	}
}

/* 
species agtDB parent: AgentDB {} 
*/

species building schedules: []{
	
	geometry shape const:true;
	rgb color <- #grey const:true;
	
	string type const:true;
	aspect base {
		draw shape color: color ;
	}
}

species stop schedules: []{
	
	geometry shape <-circle(10) const:true;
	rgb color <- #yellow const:true;
	
	string stop_id const:true;
	bool activated<-false; // variable to only display used stops 
	float passenger_arrival_rate;
	
	init{
		if length(building where (each.type="train_station") at_distance distance_from_building_tolerance)>0{
			passenger_arrival_rate<-0.7;
		}
		else{
			passenger_arrival_rate<-0.1;	
		}
	}
	
	aspect base {

		draw shape color: color border:#black;	
		
	}
	
	action call_passenger_factory{
		int n_new_passengers <- poisson(passenger_arrival_rate);
		passengers_nb<-passengers_nb+n_new_passengers;
		
		if (batch_mode and passengers_nb>0){
			add (length(passenger where each.on_board)/passengers_nb) to: passengers_ratio_list;
		}		
		
		create passenger number:n_new_passengers with:[source::self, location::self.location];
	}
}

species busLine schedules: []{
    
    rgb color const:true;
    
	string route_id const:true;
    list<stop> stops <-nil ;

    aspect base {
    	point node1;
    	point node2;
    	loop i from: 0 to:length(stops)-1 {
    		node1 <- stops[i].location;
    		node2 <- stops[i+1].location;          
            path seg <- path_between(road_graph, node1, node2);
 			draw shape(seg) color: color width: 6;
        }
    }
    
    init{
    	string file_name <- "../includes/reduced_data/"+self.route_id+".txt";
		file file_line <- csv_file(file_name, ",", string, true);
		if length(file_line)>1{
			do build_stops_list(file_line);
		}
		else{
			do die;
		}
    }
    
    action build_stops_list(file stops_file){
    	stop s;
    	loop el over: stops_file {
			s <- stop_index[string(el)];
			do update_stops_list(s);
		}
    }
    
    action update_stops_list(stop current_stop){
		stop previous_stop;
		if length(stops)>0 and current_stop != nil{
			previous_stop <- last(stops);
			do add_to_stops_list(previous_stop, current_stop);
		}
		
		else{
			do create_stops_list(current_stop);
		}
    }
    
    action create_stops_list(stop current_stop){
    	if current_stop != nil{
			current_stop.activated <- true;
			stops <- list(current_stop);
		}
    }
    
    action add_to_stops_list(stop previous_stop, stop current_stop){
    	if current_stop.location distance_to previous_stop.location >=eps{
			current_stop.activated <- true;
			add current_stop to: stops;
			/* 
			if current_stop.color = nil{
				current_stop.color <- color;
			}
			else {
				current_stop.color <- #lightgrey;
			}*/
			do update_bus_graph(previous_stop, current_stop);
		}
    }
    
    action update_bus_graph(stop previous_stop, stop current_stop){
    	if (current_stop in bus_graph.vertices) {
			current_stop <- (bus_graph.vertices where (each.location distance_to current_stop.location<eps))[0];
		}
		bus_graph <- bus_graph add_edge (previous_stop::current_stop);
    }
}

species bus parent:vehicle_base{

	rgb color const:true;
	
	// id attributes
	string bus_id const:true;
	busLine line const:true;
	
	// routing attributes
	int next_stop_index;
	int direction;
	stop next_stop;
	bool at_stop<-true;
	float counter<-0.0#s;
	
	
	// capacity attributes
	int capacity <- rnd(min_capacity, max_capacity);
	list<passenger> passengers <- [];
	
	// miscellaneous attributes
	float stop_time<-20#s;
	
	init{
		max_acceleration <- 1#m/#s/#s;
		//vehicle_length <- 12#m;
		vehicle_length <- 12#mm;
		max_speed <- 70 #km/#h;
		num_lanes_occupied <- 2;
		proba_use_linked_road <- 0.0;
	}
	
	aspect base {
		draw circle(15) color: color border: #black;
	}
	
	
	reflex move when: not at_stop{
		do drive;
		if final_target=nil{
			at_stop <- true;
		}
	}
	
	reflex reach_stop when:at_stop{
		if counter = 0.0{
			do update_next_stop;
		}
		do wait_at_stop;
	}
	
	action update_next_stop{
		do update_direction;
		next_stop_index <- next_stop_index + direction;
		next_stop <- line.stops[next_stop_index];
		final_target <- closest_to(intersection, next_stop);
		map edge_weights <- road as_map (each::each.shape.perimeter);
		if current_road != nil and current_road.linked_road != nil {
			if length(current_road.target_node.roads_out)>=2{
				edge_weights[current_road.linked_road] <- 99999 #km;
			}
		}
		do compute_path graph: road_graph with_weights edge_weights target:final_target;
	}
	
	action update_direction{
		if next_stop_index <= 0 or next_stop_index >=length(line.stops)-1{
			direction<-direction*(-1);
		}
	}
	
	action wait_at_stop{
		counter <- counter + step;
		if counter>=stop_time{
			counter<-0.0#s;
			at_stop<-false;
		}
	}
	
	action get_off(passenger p){
		remove item:p from:passengers;
	}
	
	action get_on(passenger p){
		add item:p to:passengers;
	}
	
	action set_random_initial_state{
		int current_stop_index <- rnd(1, length(line.stops)-2);
		location<- line.stops[current_stop_index].location;
		direction <- rnd(1);
		direction <- direction=1?direction:-1;
		next_stop_index <- current_stop_index+direction;
	}
	
	action set_initial_state(int current_stop_index){
		location<- line.stops[current_stop_index].location;
		direction <- 1;
		next_stop_index <- current_stop_index+direction;
	}
}

species motorbike parent:vehicle_base{
	
	rgb color <- #red const:true;
	
	init{
		max_acceleration <- 2#m/#s/#s;
		//vehicle_length <- 1.89#m;
		vehicle_length <- 1.89#mm;
		max_speed <- 70 #km/#h;
		num_lanes_occupied <- 1;
		location <- any(road_graph.vertices);
	}
	
	aspect base {
		draw circle(5) color: color border: #black;
	}
	
	reflex move{
		do drive_random graph:road_graph;		
	}
}

species car parent:vehicle_base{
	
	rgb color <- #darkcyan const:true;
	
	// example Clio V
	init{
		max_acceleration <- 1.62#m/#s/#s;
		//vehicle_length <- 4.05#m;
		vehicle_length <- 4.05#mm;
		max_speed <- 160 #km/#h;
		num_lanes_occupied <- 2;
		location <- any(road_graph.vertices);
	}
	
	aspect base {
		draw circle(10) color: color border: #black;
	}
	
	reflex move{
		if current_road != nil and current_road.linked_road != nil{
			if length(current_road.target_node.roads_out)>=2{
				int l <- length(current_target.roads_out)-1;
				map<road, float> proba_roads <- (current_target.roads_out as_map (each::1/l));
				proba_roads[current_road.linked_road] <- 0.0;
			}
			
		}
		do drive_random graph:road_graph;
	}
}

species passenger skills:[moving]{

	rgb color <- #orange among:[#orange, #green];
	
	//routing
	stop source;
	stop target;
	point next_stop_loc;
	bus current_bus <-nil;
	path way;
	int way_index <- 0;
	bool updated <- false;
	bool on_board <- false;
	
	
	//metrics
	float waiting_time <-0.0#s;
	float time_to_reach_target <-0.0#s;
	
	
	aspect base {
		if on_board{			
			draw square(30) color: color border: #black at:current_bus.location;
		}
		else{
			draw square(30) color: color border: #black;
		}
	}
	init{
		do find_valid_target;
		do update_next_stop_loc;
	}
	
	reflex update_metrics{
		time_to_reach_target <- time_to_reach_target+step;
		if not on_board{
			waiting_time <- waiting_time + step;
		}
	}
	
	reflex move when: on_board{
		
		location <- current_bus.location;

		if current_bus.at_stop{
			if location distance_to target.location<eps{
				do reach_target;
			}
			else{
				do update_next_stop;
			}
		}
		else{
			updated<-false;
		}
	}
	
	action reach_target{
		
		if current_bus != nil{
			ask current_bus{
				do get_off(myself);
			}
		}
		if (batch_mode){
			add waiting_time to: waiting_time_list ;
			add time_to_reach_target to: time_to_reach_target_list;
		}
		if (verbose_mode){
			write string(self) + " arrived at " + self.target;
			write "waiting time: " +string(waiting_time/60/step)+" min";
			write "time to reach: " +string(time_to_reach_target/60/step)+" min";
			write "";
		}
		passengers_nb<-passengers_nb-1;
		do die;
	}
	
	action update_next_stop{
		if not updated and location distance_to next_stop_loc < eps{
			way_index <- way_index + 1;
			do update_next_stop_loc;
			if current_bus.final_target.location distance_to next_stop_loc>=eps{
				do get_off;
			}
		}
	}
	
	action update_next_stop_loc{

		list cur_edge <- list(way.edges[way_index]);
		point p0 <- point(cur_edge[0]);
		point p1 <- point(cur_edge[1]);
		
		next_stop_loc <- location distance_to p0<eps? p1:p0;
		updated <- true;
	}
	
	action find_valid_target{
		target <- one_of(stop_index);
		way <- path_between(bus_graph, source, target);
		loop while: length(way.edges)=0{
			target <- one_of(stop_index);
			way <- path_between(bus_graph, source, target);
		}
	}
		
	action get_off{
		ask current_bus{
			do get_off(myself);
		}
		
		current_bus<-nil;
		on_board <- false;
		color <- #orange;
	}

	reflex wait when: not on_board{
		if location distance_to target.location<eps{
			do reach_target;
		}
		else{ 
			do request_neighborhood;
		}
	}
	
	action request_neighborhood{
		loop b over:(bus at_distance(eps)){
			if (is_valid_bus(b)){
				do get_on(b);
				break;
			}	
		}
	}
	
	bool is_valid_bus(bus b){
		stop next_s <- b.line.stops first_with (each.location distance_to next_stop_loc < eps);
		stop cur_s  <- b.line.stops first_with (each.location distance_to location < eps);
		if next_s != nil and cur_s != nil{
			return is_right_direction(b, cur_s, next_s);
		}
		return false;
	}
	
	bool is_right_direction(bus b, stop cur_s, stop next_s){
		int idx_cur  <- b.line.stops index_of cur_s;
		int idx_next <- b.line.stops index_of next_s;
		bool correct_direction <- (idx_next - idx_cur)*b.direction>0;
		return (correct_direction and (length(b.passengers)+1)<b.capacity);
	}
	
	action get_on(bus b){
		current_bus <- b;
		ask b{
			do get_on(myself);
		}
		on_board <- true;
		color <- #green;
		updated <- true;
	}
}

experiment road_traffic type: gui {
	parameter "seed: " var: int_seed min: 1 max: 100 step:1;
	parameter "passenger spawn frequency" var: spawn_frequency min: 500.0#s max: 20000.0#s step:500.0#s;
	parameter "bus number per line" var: buses_nb_per_line min:1 max:5 step:1;
	parameter "cars number" var: cars_nb min:0 max:200 step:25;
	parameter "motorbikes number" var: motorbikes_nb min:0 max:500 step:25;
	output {
		display city_display type:3d {

			species road aspect: base refresh:false;
			//species intersection aspect: base refresh:false;
			
			species passenger aspect: base;
			species bus aspect:base;
			species motorbike aspect:base;
			species car aspect:base;
			species busLine aspect: base refresh:false transparency:2/3;
			species stop aspect: base refresh:false transparency:2/3;
		}
		
		monitor "Number of people agents" value: passengers_nb;
	}
}


experiment road_traffic_with_building type: gui{
	parameter "seed: " var: int_seed min: 1 max: 100 step:1;
	parameter "passenger spawn frequency" var: spawn_frequency min: 100.0#s max: 2000.0#s step:50.0#s;
	parameter "bus number per line" var: buses_nb_per_line min:1 max:5 step:1;
	parameter "cars number" var: cars_nb min:0 max:200 step:25;
	parameter "motorbikes number" var: motorbikes_nb min:0 max:500 step:25;
	output {
		display city_display type:3d {

			species building aspect: base refresh:false;
			species road aspect: base refresh:false;
			
			species passenger aspect: base;
			species bus aspect:base;
			species motorbike aspect:base;
			species car aspect:base;
			species busLine aspect: base refresh:false transparency:2/3;
			species stop aspect: base refresh:false  transparency:2/3;
		}
		
		monitor "Number of people agents" value: passengers_nb;
	}
}


experiment batch_experiments type: batch repeat: 1 keep_seed: true until: (is_output_flag){
    parameter 'Seed:' var: int_seed min:1 max:10 step:1 init: 1;
    parameter 'Batch mode' var: batch_mode init:true among:[true];
}
