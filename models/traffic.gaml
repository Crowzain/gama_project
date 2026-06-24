/**
* Name: traffic
* Based on the internal empty template. 
* Author: pierrebeauvain
* Tags: 
*/


model traffic

/* Insert your model definition here */

species road  skills:[road_skill]{
	
	rgb color <- #black const:true;
	
	int num_lanes<-2 const:true;
	bool oneway const:true;
	road linked_road <- nil;
	
	intersection source_node <- nil;
	intersection target_node <- nil;
	
	aspect base {
		draw shape color: color;
	}
	
	action create_opposite_direction_road(road r){
		create road with:[
			maxspeed::r.maxspeed, oneway::false, 
			shape::polyline(reverse(shape.points)), name::name
		]{	
			self.linked_road <- r;
			r.linked_road <- self;
		}
	}
	
	action connect_two_intersections(intersection source, intersection target){
		source_node <- source;
		target_node <- target;
		add self to: target.roads_in;
		add self to: source.roads_out;
	}
}

species intersection  skills:[intersection_skill] {
	
	rgb color <- #pink const:true;
	
	string type const:true;
	list<road> roads_in <- [];
	list<road> roads_out <- [];
	
	aspect base {
		draw shape+2 color: color ;
	}
}

species vehicle_base virtual:true skills:[driving]{
	road current_road <-nil;
	intersection current_target <- nil;
	
	init{
		right_side_driving <- true;
		proba_respect_priorities <- 1.0;
		speed_coeff <- rnd(0.7, 1.0);
		min_safety_distance <- rnd(0.5, 5)#m;
	}
}