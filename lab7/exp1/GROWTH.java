import java.io.IOException;
import java.util.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;


public class GROWTH extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		double ratio = 0.0;
		
		Object values = input.get(0);
		if (values instanceof DataBag) {
			DataBag bag = (DataBag) values;
			// ignore a company doesn't exist for more than 1 day
			if (bag.size() < 2) {
				return null;
			}
			
			else {
				Iterator<Tuple> it = ((DataBag) values).iterator();
				// get the first tuple
				// tuple: ticker,date,open price
				Tuple firstTuple = it.next();
				
				// get the date of the first tuple
				// set this date as the beginning and ending date
				int begin_date = (Integer)firstTuple.get(1);
				int end_date = begin_date;
				
				// get the price of the first tuple
				double begin_price = (Double)firstTuple.get(2);
				double end_price = begin_price;
				
				// set the ratio to be 1.0
				ratio = 1.0;
				
				while (it.hasNext()) {
					// tuple: ticker,date,open
					Tuple t = it.next();
					// get the date and price of that tuple
					int date = (Integer)t.get(1);
					double price = (Double)t.get(2);
					
					if (date<begin_date) {
						begin_date = date;
						begin_price = price;
					}
					if (date>end_date) {
						end_date = date;
						end_price = price;
					}
				}
				
				ratio = end_price/begin_price;	
				return ratio;
			}
		}
		
		return null;
	}
	
	

}
