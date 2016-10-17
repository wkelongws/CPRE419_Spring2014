import java.io.IOException;
import java.util.*;
import java.util.Map.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class MOVAVG extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		String tickerName = (String)input.get(0); 
		Object values = input.get(1);
		
		BagFactory bf = BagFactory.getInstance();
		TupleFactory tf = TupleFactory.getInstance();
		DataBag db = bf.newDefaultBag();
		
		if (values instanceof DataBag) {
			double moving_avg = 0.0;
			
			Iterator<Tuple> it = ((DataBag) values).iterator();
			// first tree map is for tuples with date from 20130801 to 20130930
			TreeMap<Integer,Double> tm1 = new TreeMap<Integer,Double>();
			// second tree map is for tuples with date from 20131001 to 20131031
			TreeMap<Integer,Double> tm2 = new TreeMap<Integer,Double>();
			
			while (it.hasNext()) {
				// tuple: ticker,date,price
				Tuple t = it.next();
				// get the date and price of that tuple
				int date = (Integer)t.get(1);
				double price = (Double)t.get(2);
				if (date<=20130930) {
					tm1.put(date, price);
				}
				else {
					tm2.put(date, price);
				}
			}
			
			if (tm1.size() < 20) {
				return null;
			}
			
			// prices is an array to store the prices of last 20 business days before 20131001
			// plus the prices in October
			int length = 20 + tm2.size();
			double[] prices = new double[length];
			
			// get the last 20 prices in tm1
			for (int i=0; i<20; i++) {
				prices[19-i] = tm1.pollLastEntry().getValue();
				moving_avg += prices[19-i];
			}
			// moving average price for the first date in tm2
			// not necessary to be Oct. 1st
			moving_avg = moving_avg/20;
			
			for (int i=0; i<tm2.size(); i++) {
				Entry<Integer,Double> e = tm2.pollFirstEntry();
				// the first available date in Oct.
				int key = e.getKey();
				// the open price on that date
				double value = e.getValue();
				
				// create a new tuple with 4 fields: 
				// ticker, date, open price, moving average price 
				Tuple t = tf.newTuple(4);
				
				// set the ticker
				t.set(0, tickerName);
				// set the date
				t.set(1, key);
				// set the open price
				t.set(2, value);
				// set the moving average price
				// keep the last 4 digits
				t.set(3, Math.round(moving_avg*1e4)/1e4);
				// add this new tuple to the databag that will be returned
				db.add(t);
				
				// calculate the new moving average price for the next tuple
				moving_avg = (moving_avg*20 - prices[i] + value) / 20;
				prices[20+i] = value;			
			}
			
			return db;					
		}
		
		return null;
	}
	
}
