/**
* Name: projectZZ3
* Based on the internal empty template. 
* Author: pierrebeauvain
* Tags: 
*/


model projectZZ3

global {
	
	file shape_file_buildings <- shape_file("../includes/reduced_data/reduced_buildings.shp");
	file shape_file_roads <- shape_file("../includes/reduced_data/reduced_roads.shp");
	file file_stops<-shape_file("../includes/reduced_data/reduced_stops.shp");
	file file_lines<-csv_file("../includes/reduced_data/lines.txt", ",", "'", true);
	
	geometry shape <- envelope(shape_file_roads);
	int nb_groups <- 5;
	int n_max_people <- 10 const:true;
	graph road_graph <- as_edge_graph(shape_file_roads);
	graph bus_graph <-graph([]);
	
	
	
	map<string,stop> stop_index <- [];
	map<string,stop> bus_index <- [];

	/* Database settings
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
		step <- 1#s;
		seed<-4.0;
		create building from: shape_file_buildings;

		create road from: shape_file_roads with:[maxspeed::float(read('maxspeed'))];
		
		
		create stop from:file_stops with:[stop_id::string(read ('stop_id')), location::point(read('geom'))];
		ask stop {
    		stop_index[stop_id] <- self;
    		location <- shape_file_roads closest_to(self);
		}

		create busLine from:file_lines with:[route_id::string (read ('name'))]{
			route_color <- rnd_color(255);
			string file_name <- "../includes/reduced_data/"+self.route_id+".txt";
			file file_line <- csv_file(file_name, ",", "'", true);
			stop s;
			point node1;
			point node2;
			
			// get route_id stored into the header
			route_id <- file_line.attributes[0];
			loop el over: file_line {
				s <- stop_index[string(el)];
				s.activated <-true;
				
				// case when stops list is empty
				if length(stops)>0 and s != nil{
					add s to: stops;
					if length(stops)>1{
						
						node1 <- s.location;
						
						// bus_graph construction must be improved/fixed
						bus_graph <- bus_graph add_edge (node2::node1);
						node2 <- node1;
					}
				}
				
				else{
					if s != nil{
						stops <- list(s);
						node1 <- s.location;
						node2 <- node1;

					}
	
				}
				
			}
			create bus with:[location::one_of(stops).location, line::self];
		}
		create passenger number:5{
			source <- one_of(stop_index where (each.activated=true));
			target <- one_of(stop_index);

			location<-source.location;
			
			//again bus_graph must be modified, sometimes way is empty, whereas graph is connected (a path between each pair of vertices does exist)
			way <- path_between(bus_graph, closest_to(bus_graph.vertices, source), closest_to(bus_graph.vertices, target));
				

		}
	}
}




species building{
	
	string type;

	rgb color <- #grey ;
	aspect base {
		draw shape color: color ;

	}
}

species stop {
	rgb color <- #yellow;
	string stop_id;
	bool activated<-false; // variable to only display used stops (maybe the other could die, but I have not found an efficient way for that)
	
	aspect base {
		if activated {
			draw circle(10) color: color border: #black;	
		}
	}
}

	
species busLine{
	string route_id;
    list<stop> stops <-nil ;
    path paths;
    rgb route_color;
    
    

    aspect base {
    	location <- stops[0].location;
    	point node1;
    	point node2;
    	loop i from: 0 to:length(stops) {
    		node1 <- stops[i].location;
    		node2 <- stops[i+1].location;          
            path seg <- path_between(road_graph, node1, node2);
 			draw shape(seg) color: route_color width: 6;
        }
    }
}

species bus skills:[moving]{
	string bus_id;
	busLine line;
	int next_stop_index <- 1;
	int capacity <- rnd(n_max_people) min:10 max:30;
	list<passenger> passengers <- [];
	float stop_time<-0.0;
	int direction <- 1;
	
	stop target <- line.stops[next_stop_index];
	
	aspect base {
		draw circle(20) color: line.route_color border: #black;
	}
	
	
	reflex move{
		speed <- 30 #km/#h;
		current_path <- goto(target:target, on:road_graph, return_path:true);
		if location = target.location or current_path=nil{
			next_stop_index <- (next_stop_index+direction) ;
			target <- line.stops[next_stop_index];
			if next_stop_index = length(self.line.stops)-1 or next_stop_index = 0{
				direction <- direction*-1;
			}
		}
	}	
}


// just a try to embed bus with driving skill

/*
species bus skills:[driving]{
	string bus_id;
	busLine line;
	int next_stop_index <- 1;
	int capacity <- rnd(n_max_people) min:10 max:30;
	list<passenger> passengers <- [];
	float stop_time<-0.0;
	int direction <- 1;
	
	stop target <- line.stops[next_stop_index];
	
	aspect base {
		draw circle(20) color: line.route_color border: #black;
	}
	
	action select_target_path {
		if line.stops[next_stop_index]=nil {
			next_stop_index <- next_stop_index+direction;
		}
		target <- line.stops[next_stop_index];
		do compute_path graph: road_graph target: road_graph.vertices closest_to target;
		next_stop_index <- (next_stop_index+direction) ;
			target <- line.stops[next_stop_index];
			if next_stop_index = length(self.line.stops)-1 or next_stop_index = 0{
				direction <- direction*-1;
			} 
	}
	
	reflex choose_path when: (target = nil) or (location distance_to target.location < 1){
		do select_target_path;
			
	}
	
	
	reflex move when: (location distance_to target.location >= 1){
		do drive;
		
	}	
}*/


species passenger skills:[moving]{
	rgb color <- #orange ;
	stop source;
	stop target;
	bus current_bus <-nil;
	path way;
	int way_index <- 1;
	
	
	aspect base {
		draw square(50) color: color border: #black;
	}
	
	
	reflex get_on when: current_bus =nil{
		ask bus at_distance(5){
			  if target.location distance_to point(myself.way.vertices[myself.way_index])<0.01{
				myself.current_bus <- self;
				add myself to: passengers;
			}
		}
	}
	
	
	reflex move when: current_bus !=nil{
		location <- current_bus.location;
		
		
		// conditon needs to be fixed to handle target point (way.vertices does not return the last vertex of the path) 
		if location distance_to target.location<1 or way_index=length(way.vertices)-1{
			do die;
		}
		if location distance_to point(way.vertices()[way_index]) <0.01 {
			way_index <- way_index + 1;
			current_bus <- nil;
		}
	}
}


// should be improved to be used and to store vehicle on it for example
species road  skills:[road_skill]{
	rgb color <- #black ;
	aspect base {
		draw shape color: color ;
	}
}

experiment road_traffic type: gui {
	parameter "Shapefile for the buildings:" var: shape_file_buildings category: "GIS" ;
	parameter "Shapefile for the roads:" var: shape_file_roads category: "GIS" ;
	parameter "Shapefile for the bounds:" var: shape_file_roads category: "GIS" ;
	//parameter "Number of people agents" var: nb_groups category: "People" ;
	//parameter "Number of bus lines" var: nb_bus_lines category: "Bus";

	output {
		display city_display type:3d {
			species building aspect: base refresh:false;
			species road aspect: base refresh:false;
			species passenger aspect: base;
			species busLine aspect: base refresh:false;
			species stop aspect: base refresh:false;
			species bus aspect:base ;
			
		}
	}
}