package gr.katsip.synefo.storm.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

public class PunctuationTuple implements Tuple {
	
	private int sourceTask;
	
	private String sourceStream;

	private HashMap<String, Object> _values;
	
	public PunctuationTuple() {
		_values = new HashMap<String, Object>();
	}
	
	public PunctuationTuple(int sourceTask, String sourceStream) {
		this.sourceStream = sourceStream;
		this.sourceTask = sourceTask;
	}
	
	@Override
	public boolean contains(String field) {
		if(_values.containsKey(field))
			return true;
		else
			return false;
	}

	@Override
	public int fieldIndex(String field) {
		int idx = 0;
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, Object> pair = itr.next();
			if(pair.getKey().equals(field))
				return idx;
			else
				idx += 1;
		}
		return -1;
	}

	@Override
	public byte[] getBinary(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBinaryByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean getBoolean(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean getBooleanByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Byte getByte(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Byte getByteByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDouble(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDoubleByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getFields() {
		ArrayList<String> field = new ArrayList<String>();
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, Object> pair = itr.next();
			field.add(pair.getKey());
		}
		return new Fields(field);
	}

	@Override
	public Float getFloat(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Float getFloatByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getInteger(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getIntegerByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long getLong(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long getLongByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageId getMessageId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Short getShort(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Short getShortByField(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSourceComponent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GlobalStreamId getSourceGlobalStreamid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSourceStreamId() {
		return sourceStream;
	}

	@Override
	public int getSourceTask() {
		return sourceTask;
	}

	@Override
	public String getString(int idx) {
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		int index = 0;
		while(itr.hasNext()) {
			if(index == idx) {
				Entry<String, Object> entry = itr.next();
				return (String) entry.getValue();
			}else {
				index += 1;
			}
		}
		return null;
	}

	@Override
	public String getStringByField(String field) {
		if(_values.containsKey(field)) {
			return (String) _values.get(field);
		}else {
			return null;
		}
	}

	@Override
	public Object getValue(int i) {
		int idx = 0;
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, Object> pair = itr.next();
			if(idx == i) {
				return pair.getKey();
			}else {
				idx += 1;
			}
		}
		return null;
	}

	@Override
	public Object getValueByField(String field) {
		if(_values.containsKey(field)) {
			return _values.get(field);
		}else {
			return null;
		}
	}

	@Override
	public List<Object> getValues() {
		ArrayList<Object> field = new ArrayList<Object>();
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, Object> pair = itr.next();
			field.add(pair.getValue());
		}
		return field;
	}

	@Override
	public List<Object> select(Fields fields) {
		ArrayList<Object> fieldValues = new ArrayList<Object>();
		Iterator<Entry<String, Object>> itr = _values.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, Object> pair = itr.next();
			if(fields.contains((String) pair.getKey())) {
				fieldValues.add(pair.getValue());
			}
		}
		return fieldValues;
	}

	@Override
	public int size() {
		return _values.size();
	}

}
