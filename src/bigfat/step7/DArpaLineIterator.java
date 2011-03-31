package bigfat.step7;

import java.io.InputStream;
import java.nio.charset.Charset;

import bigfat.util.UncheckedLineIterable.UncheckedLineIterator;

public class DArpaLineIterator extends UncheckedLineIterator {

    private int prevOrder = -1;
    private int order = -1;
    private String name;
	
    public DArpaLineIterator(InputStream istream, Charset charset, String name) {
		super(istream, charset);
		this.name = name;
	}

	private static int getOrder(String str) {
		int order = 1;
		for(int i=0; i<str.length(); i++) {
			char c = str.charAt(i);
			if(c == '\t') {
				break;
			}
			if(c == ' ') {
				order++;
			}
		}
		return order;
	}
	
	public int getOrder() {
	    if(order == -1 && super.hasNext()) {
			order = getOrder(super.peek());
			if(order < prevOrder) {
			    throw new RuntimeException("order < prevOrder in " + name + ": " + order + " < " + prevOrder);
			}
		}
		return order;
	}
	
	@Override
	public String next() {
	    prevOrder = order;
		order = -1;
		return super.next();
	}

    public String getName() {
	return name;
    }

}
