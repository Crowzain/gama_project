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
	float time_step <- 1 #mn;
	int nb_groups <- 5;
	int n_max_people <- 100 const:true;
	graph the_graph <- as_edge_graph(shape_file_roads);
	//the_graph <- the_graph with_weights (my_graph.edges as_map (each::geometry(each).length));
	
	map<string,stop> stop_index <- [];
	map<string,stop> bus_index <- [];
	
	float tolerance <- 1.0;
	bool split_lines <- true;
	bool keep_main_connected <- true;
	
	map<string,string> MYSQL <- [
					'host'::'127.0.0.1',
					'dbtype'::'MySQL',
					'database'::'mysql_db',
					'port'::'3306',
					'user'::'root',
					'passwd'::'7dae25e34ecd1e45fcb5738bfb69b6665299ae32'];
	
	map <string, string>  SQLITE <- [
    	'dbtype'::'sqlite',
    	'database'::'../includes/gama_project_sqlite.db'];
	string QUERY <- "SELECT stop_id FROM stops";
	
	init {		
		create building from: shape_file_buildings;

		create road from: shape_file_roads;// with:[dist::float(read('length'))];
		
		
		create stop from:file_stops with:[stop_id::string(read ('stop_id')), location::point(read('geom'))];
		ask stop {
    		stop_index[stop_id] <- self;
    		location <- shape_file_roads closest_to(self);
		}

		create busLine from:file_lines with:[route_id::string (read ('name'))]{
			route_color <- rnd_color(255);
			string file_name <- "../includes/reduced_data/"+self.route_id+".txt";
			file file_line <- csv_file(file_name, ",", "'", true);
			loop el over: file_line {
				if stops != nil {
					
					stops <- stops + stop_index[string(el)];
				}
				else{
					if string(el) != nil{
						stops <- list(stop_index[string(el)]);
						

					}
					else{
						route_id <- el;
						
					}
				}
			}
			create bus with:[location::stops[0].location, line::self];
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
	
	aspect base {
		draw circle(10) color: color border: #black;
	}
}

	
species busLine{
	string route_id;
    list<stop> stops <-nil ;
    path paths;
    rgb route_color;
    
    

    aspect base {
    	location <- stops[0].location;
    	loop i from: 0 to:length(stops) {
            // Project stops onto the graph
            //point p1 <- closest_to(graph_node(the_graph), stops[i].location);
            //

            // Compute path on the graph
            
            path seg <- path_between(the_graph, stops[i].location, stops[i+1].location);
 			draw shape(seg) color: route_color width: 6;
        }
       
        
    }
}

species bus skills:[moving]{
	string bus_id;
	busLine line;
	int next_stop_index <- 1;
	int capacity <- rnd(n_max_people) min:10 max:30;
	int load <- 0;
	float stop_time<-0.0;
	int direction<-1;
	
	stop target <- line.stops[next_stop_index];
	
	aspect base {
		draw circle(20) color: line.route_color border: #black;
	}
	
	
	reflex move{
		speed <- 30 #km/#h;
		current_path <- goto(target:target, on:the_graph, return_path:true);
		if location = target.location or current_path=nil{
			next_stop_index <- (next_stop_index+direction) ;
			target <- line.stops[next_stop_index];
			if next_stop_index = length(self.line.stops)-1 or next_stop_index = 0{
				direction <- direction*-1;
			}
		}
		
	}
	
	
}

/*species db_agent parent: AgentDB {
	//insert your descriptions here
}*/



/*species passengersGroup {
	rgb color <- #yellow ;
	int size <- rnd(n_max_people) min:0 max:n_max_people;
	aspect base {
		draw square(20) color: color border: #black;
	}
}*/

species road  {
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
			species building aspect: base ;
			species road aspect: base ;
			//species passengersGroup aspect: base ;
			//species busLine aspect: base ;
			species stop aspect: base ;
			species bus aspect:base ;
			
		}
	}
}