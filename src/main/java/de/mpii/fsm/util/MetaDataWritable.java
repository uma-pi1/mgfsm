package de.mpii.fsm.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Bundles a document identifier and its corresponding timestamp.
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class MetaDataWritable implements Writable {

    private long did;

    private long ts;

    private Map<String, String> metaData = Collections.emptyMap();

    public MetaDataWritable() {
    }

    public MetaDataWritable(long did, long ts) {
        this.did = did;
        this.ts = ts;
    }

    public MetaDataWritable(long did, long ts, Map<String, String> metaData) {
        this.did = did;
        this.ts = ts;
        this.metaData = metaData;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeVLong(d, did);
        WritableUtils.writeVLong(d, ts);
        WritableUtils.writeVInt(d, metaData.size());
        for (String key : metaData.keySet()) {
            WritableUtils.writeCompressedString(d, key);
            WritableUtils.writeCompressedString(d, metaData.get(key));
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        did = WritableUtils.readVLong(di);
        ts = WritableUtils.readVLong(di);
        int metaDataLength = WritableUtils.readVInt(di);
        if (metaDataLength > 0) {
            metaData = new HashMap<String, String>();
            for (int i = 0; i < metaDataLength; i++) {
                metaData.put(WritableUtils.readCompressedString(di), WritableUtils.readCompressedString(di));
            }
        }
    }

    public long getDId() {
        return did;
    }

    public void setDId(long did) {
        this.did = did;
    }

    public long getTS() {
        return ts;
    }

    public void setTS(long ts) {
        this.ts = ts;
    }

    public void set(MetaDataWritable m) {
        did = m.getDId();
        ts = m.getTS();
        if (!m.metaData.isEmpty()) {
            metaData = new HashMap<String, String>();
            for (String key : m.metaData.keySet()) {
                metaData.put(key, m.metaData.get(key));
            }
        }
    }

    public void setMetaData(String key, String value) {
        if (metaData.isEmpty()) {
            metaData = new HashMap<String, String>();
        }
        metaData.put(key, value);
    }

    public String getMetaData(String key) {
        return metaData.get(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MetaDataWritable other = (MetaDataWritable) obj;
        if (this.did != other.did) {
            return false;
        }
        if (this.ts != other.ts) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + (int) (this.did ^ (this.did >>> 32));
        hash = 83 * hash + (int) (this.ts ^ (this.ts >>> 32));
        return hash;
    }

    @Override
    public String toString() {
        return "(" + did + ", " + ts + ")";
    }

}
