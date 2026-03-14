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
	graph<stop,stop> bus_graph <-graph([]);
	float eps <- 0.1;
	
	
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
			stop node1;
			stop node2;
			
			// get route_id stored into the header
			route_id <- file_line.attributes[0];
			loop el over: file_line {
				s <- stop_index[string(el)];
				s.activated <-true;
				
				// case when stops list is empty
				if length(stops)>0 and s != nil{
					add s to: stops;
						
					
					if s in bus_graph.vertices{
						node1 <- (bus_graph.vertices where (each.location=s.location))[0];
					}
					else{
						node1 <- s;
					}
					// bus_graph construction must be improved/fixed
					bus_graph <- bus_graph add_edge (node2::node1);
					node2 <- node1;
				}
				
				else{
					if s != nil{
						stops <- list(s);
						node2 <- s;

					}
	
				}
				
			}
			create bus with:[location::stops[0].location, line::self];
		}
		create passenger number:10{
			source <- one_of(stop_index where (each.activated=true));
			target <- one_of(stop_index where (each.activated=true));

			location<-source.location;
			
			way <- path_between(bus_graph, source, target);
			
			// while path is empty, new target is generated
			loop while: length(way.vertices)=0{
				target <- one_of(stop_index where (each.activated=true));
				way <- path_between(bus_graph, source, target);
			}
			list cur_edge <- list(way.edges[0]);
			point p0 <- point(cur_edge[0]);
			point p1 <- point(cur_edge[1]);
			
			next_stop_loc <- location distance_to p0 <eps? p1:p0; // merge node in the same location

		}
		
		//filter unused stop by killing them
		ask stop {
			if not activated{
				do die;
			}
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
	bool activated<-false; // variable to only display used stops 
	
	aspect base {
		if activated {
			draw circle(10) color: color border: #black;	
		}
	}
}

	
species busLine{
	string route_id;
    list<stop> stops <-nil ;
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
	int capacity <- rnd(n_max_people) min:10 max:30;
	list<passenger> passengers <- [];
	
	// miscellaneous attributes
	float stop_time<-20#s;
	
	
	aspect base {
		draw circle(20) color: line.route_color border: #black;
	}
	
	
	reflex move when: not at_stop{
		speed<-30 #km/#h;
		do goto(target:next_stop, on:road_graph, return_path:false);
		if location distance_to next_stop < eps{
			at_stop <- true;
		}
	}
	
	reflex updateNextStop when:	at_stop{
		
		if counter = 0.0{
			if next_stop_index in [0, length(line.stops)-1]{
				direction<-direction*(-1);
			}
			next_stop_index <- next_stop_index + direction;
			next_stop <- line.stops[next_stop_index];
		}
		counter <- counter + step;
		if counter>=stop_time{
			counter<-0.0#s;
			at_stop<-false;
		}
		
	}
}



species passenger skills:[moving]{
	rgb color <- #orange ;
	stop source;
	stop target;
	point next_stop_loc;
	bus current_bus <-nil;
	path way;
	int way_index <- 0;
	bool updated <- false;
	
	bool on_board <- false;
	
	
	aspect base {
		draw square(50) color: color border: #black;
	}
	
	
	reflex move when: on_board{
		
		location <- current_bus.location;

		
		if current_bus.at_stop{
			if location distance_to target.location >=eps{
				if not updated{
					if location distance_to next_stop_loc <eps{
						way_index <- way_index + 1;
						
						list cur_edge <- list(way.edges[way_index]);
						point p0 <- point(cur_edge[0]);
						point p1 <- point(cur_edge[1]);
						
						next_stop_loc <- location distance_to p0 <eps? p1:p0;
						if current_bus.next_stop.location distance_to next_stop_loc>=eps{
							current_bus<-nil;
							on_board <- false;
						}
						updated <- true;
					}
				}
			}
			else{
				write string(self) + " arrived at " + self.target;
				do die;
			}
		}
		else{
			updated<-false;
		}
	}
	
	
	reflex request when: not on_board{
		if location distance_to target.location<eps{
			write string(self) + " arrived at " + self.target;
			do die;
		}
		else{
			ask bus at_distance(eps){
				stop next_s <- line.stops first_with (each.location distance_to myself.next_stop_loc < eps);
        		stop cur_s  <- line.stops first_with (each.location distance_to myself.location < eps);
				if next_s != nil and cur_s != nil{
					int idx_cur  <- line.stops index_of cur_s;
					int idx_next <- line.stops index_of next_s;
					bool correct_direction <- (idx_next - idx_cur)*direction>0;
					if correct_direction and length(passengers)+1<capacity{
						myself.current_bus <- self;
						add myself to: passengers;
						myself.on_board <- true;
						myself.updated <- true;
					}
				}
			}
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
			species busLine aspect: base refresh:false transparency:0.5;
			species stop aspect: base refresh:false;
			species bus aspect:base ;
			
		}
	}
}