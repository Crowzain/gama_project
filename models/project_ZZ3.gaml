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
	file file_lines<-csv_file("../includes/reduced_data/lines.txt", ",", "'", true) const:true;
	geometry shape <- envelope(shape_file_roads);
	
	//parameters
	int initial_passengers_nb <- 10 const:true;
	int passengers_nb <- initial_passengers_nb;
	int min_capacity <- 5 const:true;
	int max_capacity <- 30 const:true;
	float eps <- 0.1 const:true;
	bool verbose <- false const:true;
	bool buildings_mode<- false const:true;
	float T_max <- 6 #h const:true;
	float lam<-0.0;
	float spawn_frequency<-6.5#mn;
	
	list<float> waiting_time_list <-[];
	list<float> time_to_reach_target_list <-[];
	
	
	
	list<road> open_roads;
	

	//graphs	
	graph road_graph <- as_edge_graph(shape_file_roads) const:true;
	graph<stop,stop> bus_graph <-graph([]);
	
	//index map
	map<string,stop> stop_index <- [];
	
	reflex stop_simulation when: (time >= T_max or (passengers_nb = 0 and lam = 0)) {
		do pause;
	}
	
	reflex spawn when: every(spawn_frequency) {
		int n_new_passengers <- poisson(lam);
		do passenger_factory(n_new_passengers);
		passengers_nb<-passengers_nb+n_new_passengers;
	}
	
	action filter_stops{
		ask stop {
			if not activated{
				remove from:stop_index index:self.stop_id;
				do die;
			}
		}
	}
	
	action passenger_factory(int n_new_passengers){
		create passenger number:n_new_passengers{
			source <- one_of(stop_index);
			location<-source.location;
			
			do find_valid_target;
			do update_next_stop_loc;
		}
	}
	
	action fill_stop_index_map{
		ask stop {
    		stop_index[stop_id] <- self;
    		location <- shape_file_roads closest_to(self);
		}
	}
	
	action update_motorbike_population (int new_number) {
			int delta <- length(motorbike) - new_number;
			if (delta > 0) {
				ask delta among motorbike {
					do unregister;
					do die;
				}
	
			} else if (delta < 0) {
				create motorbike number: -delta ;
			}
		}
		action update_car_population (int new_number) {
			int delta <- length(car) - new_number;
			if (delta > 0) {
				ask delta among car {
					do unregister;
					do die;
				}
	
			} else if (delta < 0) {
				create car number: -delta ;
			}
	
		}
	action update_road_scenario (int scenario) {
		open_roads <- scenario = 1 ? road where !each.s1_closed : (scenario = 2 ? road where !each.s2_closed : list(road));
		// Change the display of roads
		list<road> closed_roads <- road - open_roads;
		ask open_roads {
		closed <- false;
		}
	
		ask closed_roads {
			closed <- true;
		}
	
		ask agents of_generic_species vehicle {
			do unregister;
			if (current_road in closed_roads) {
				do die;
			}
	
		}
	
		ask building {
			closest_intersection <- nil;
		}
	
		ask intersection {
			do die;
		}
		
		graph g <- as_edge_graph(open_roads);
	
		write g;
		loop pt over: g.vertices {
			create intersection with: (shape: pt);
		}
	
		ask building {
			closest_intersection <- intersection closest_to self;
		}
		ask road {
			vehicle_ordering <- nil;
		}
	//build the graph from the roads and intersections
	road_network <- as_driving_graph(open_roads, intersection) with_shortest_path_algorithm #FloydWarshall;
	//geometry road_geometry <- union(open_roads accumulate (each.shape));
	ask agents of_generic_species vehicle {
		do select_target_path;
	} 
}
	
	/* 
	//Database settings
	map<string,string> MYSQL <- [
					'host'::'127.0.0.1',
					'dbtype'::'MySQL',
					'database'::'mysql_db',
					'port'::'3306',
					'user'::'root',
					'passwd'::'Iamastrongpassword']; // is it possible to access environment variable ?
	map <string, string>  SQLITE <- [
    	'dbtype'::'sqlite',
    	'database'::'../includes/gama_project_sqlite.db'];
	string QUERY <- "SELECT stop_id FROM stops";
	*/
	
	
	
	init {		
		step <- 5#s;
		//seed<-4.0;
		if (buildings_mode){
			create building from: shape_file_buildings;
		}
		
		
		
		create road from: shape_file_roads with:[maxspeed::float(read('maxspeed'))];
		
		ask road {
			agent ag <- building closest_to self;
			float dist <- ag = nil ? 8.0 : max(min( ag distance_to self - 5.0, 8.0), 2.0);
			num_lanes <- int(dist / lane_width);
			capacity <- 1 + (num_lanes * shape.perimeter/3);
		}
		int cars <- 500;
		int motos <- 1000;
		
		do update_car_population(cars);
		do update_motorbike_population( motos);
		do update_road_scenario(0); 
		
		loop r over: road {
			if (!r.oneway) {
				create road with: (shape: polyline(reverse(r.shape.points)), name: r.name, type: r.type, s1_closed: r.s1_closed, s2_closed: r.s2_closed);
			} 
		}

		
		
		create stop from:file_stops with:[stop_id::string(read ('stop_id')), location::point(read('geom'))];
		
		do fill_stop_index_map;

		create busLine from:file_lines with:[route_id::string (read ('name'))]{
			route_color <- rnd_color(255);
			string file_name <- "../includes/reduced_data/"+self.route_id+".txt";
			file file_line <- csv_file(file_name, ",", "'", true);
			
			route_id <- file_line.attributes[0]; // get route_id stored into the header
			do build_stops_list(file_line);
			
			create bus with:[location::stops[0].location, line::self];
		}
		
		do filter_stops;
		
		do passenger_factory(passengers_nb);
	}
}



species building{
	
	string type const:true;
	intersection closest_intersection <- intersection closest_to self;
	rgb color <- #grey const:true;
	aspect base {
		draw shape color: color ;

	}
}
/* 
species stop {
	rgb color <- #yellow const:true;
	string stop_id;
	bool activated<-false; // variable to only display used stops 
	
	aspect base {
		if activated {
			draw circle(10) color: color border: #black;	
		}
	}
}*/

	
species busLine{
	string route_id;
    list<stop> stops <-nil ;
    rgb route_color;

    aspect base {
    	point node1;
    	point node2;
    	loop i from: 0 to:length(stops) {
    		node1 <- stops[i].location;
    		node2 <- stops[i+1].location;          
            path seg <- path_between(road_graph, node1, node2);
 			draw shape(seg) color: route_color width: 6;
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

species bus parent:vehicle{
	/* attributes */
	// id attributes
	string bus_id;
	busLine line;
	
	// routing attributes
	int next_stop_index <- 1;
	int direction <- 1;
	stop next_stop <- line.stops[next_stop_index];
	bool at_stop<-true;
	float counter<-0.0#s;
	
	
	// capacity attributes
	int capacity <- rnd(min_capacity, max_capacity);
	list<passenger> passengers <- [];
	
	// miscellaneous attributes
	float stop_time<-20#s;
	
	
	aspect base {
		draw circle(20) color: line.route_color border: #black;
	}
	

	reflex move when: not at_stop{
		speed<-30 #km/#h;
		do drive;
		if location distance_to next_stop < eps{
			at_stop <- true;
			shift_pt <- compute_position();
		}
	}
	
	reflex reach_stop when:at_stop{
		if counter = 0.0{
			do update_next_stop;
		}
		do wait_at_stop;
	}
	
	action update_next_stop{
		if next_stop_index in [0, length(line.stops)-1]{
			direction<-direction*(-1);
		}
		next_stop_index <- next_stop_index + direction;
		next_stop <- line.stops[next_stop_index];
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
}



species passenger skills:[moving]{
	/* attributes */
	
	rgb color <- #orange ;
	
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
		draw square(50) color: color border: #black;
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
		add waiting_time to: waiting_time_list ;
		add time_to_reach_target to: time_to_reach_target_list;
		if (verbose){
			write string(self) + " arrived at " + self.target;
			write "waiting time: " +string(waiting_time/120)+" min";
			write "time to reach: " +string(time_to_reach_target/120)+" min";
			write "";
		}
		passengers_nb<-passengers_nb-1;
		do die;
	}
	
	action update_next_stop{
		if not updated{
			if location distance_to next_stop_loc < eps{
				way_index <- way_index + 1;
				do update_next_stop_loc;
				if current_bus.next_stop.location distance_to next_stop_loc>=eps{
					do get_off;
				}
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
		remove item:self from:current_bus.passengers;
		
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
			if is_valid_bus(b){
				do get_on(b);
				break;
			}	
		}
	}
	
	action is_valid_bus(bus b){
		stop next_s <- b.line.stops first_with (each.location distance_to next_stop_loc < eps);
		stop cur_s  <- b.line.stops first_with (each.location distance_to location < eps);
		if next_s != nil and cur_s != nil{
			int idx_cur  <- b.line.stops index_of cur_s;
			int idx_next <- b.line.stops index_of next_s;
			bool correct_direction <- (idx_next - idx_cur)*b.direction>0;
			return correct_direction and length(b.passengers)+1<b.capacity;
		}
		return false;
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


// should be improved to be used and to store vehicle on it for example
/*species road  skills:[road_skill]{
	rgb color <- #black ;
	aspect base {
		draw shape color: color ;
	}
}*/

experiment road_traffic type: gui {
	parameter "seed: " var: seed min: 0.0 max: 1000.0 step:1.0;
	parameter "passenger spawn poisson parameter" var: lam min: 0.0 max: 30.0 step:1.0;
	parameter "passenger spawn frequency" var: spawn_frequency min: 100.0#s max: 2000.0#s step:50.0#s;
	output {
		display city_display type:3d {

			species building aspect: base refresh:false;
			species road aspect: base refresh:false;
			
			species passenger aspect: base;
			species bus aspect:base;
			species busLine aspect: base refresh:false transparency:2/3;
			species stop aspect: base refresh:false;
			
			
		}
		
		monitor "Number of people agents" value: passengers_nb;
		monitor "Average waiting time" value: mean(waiting_time_list)/120;
		monitor "Average time to reach target" value: mean(time_to_reach_target_list)/120;
		
		
	}
}