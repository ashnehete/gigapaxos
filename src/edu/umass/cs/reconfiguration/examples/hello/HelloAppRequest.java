package edu.umass.cs.reconfiguration.examples.hello;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;

public class HelloAppRequest extends JSONPacket implements ReconfigurableRequest, ReplicableRequest, ClientRequest {

    public enum PacketType implements IntegerPacketType {
        WRITE_REQUEST(0),

        APPEND_REQUEST(1),

        DELETE_REQUEST(2)
        ;


        private static HashMap<Integer, HelloAppRequest.PacketType> numbers = new HashMap<Integer, HelloAppRequest.PacketType>();
        /* *****BEGIN static code block to ensure correct initialization ****** */
        static {
            for (HelloAppRequest.PacketType type : HelloAppRequest.PacketType.values()) {
                if (!HelloAppRequest.PacketType.numbers.containsKey(type.number)) {
                    HelloAppRequest.PacketType.numbers.put(type.number, type);
                } else {
                    assert (false) : "Duplicate or inconsistent enum type";
                    throw new RuntimeException(
                            "Duplicate or inconsistent enum type");
                }
            }
        }

        /*  *************** END static code block to ensure correct
         * initialization ************* */

        private final int number;

        PacketType(int t) {
            this.number = t;
        }

        @Override
        public int getInt() {
            return this.number;
        }

        /**
         * @param type
         * @return PacketType from int type.
         */
        public static HelloAppRequest.PacketType getPacketType(int type) {
            return HelloAppRequest.PacketType.numbers.get(type);
        }
    }

    public enum Keys {
        NAME, TEXT, TYPE, EPOCH, ID, RVAL
    }

    private final String name;
    private final String text;
    private final int epoch;
    private final long id;


    public HelloAppRequest(String name, String text, int epoch, long id, IntegerPacketType type) {
        super(type);
        this.name = name;
        this.text = text;
        this.epoch = epoch;
        this.id = id;
    }

    public HelloAppRequest(JSONObject json) throws JSONException {
        super(json);
        this.name = json.getString(Keys.NAME.toString());
        this.text = json.getString(Keys.TEXT.toString());
        this.epoch = json.getInt(Keys.EPOCH.toString());
        this.id = json.getLong(Keys.ID.toString());

        this.response = json.has(Keys.RVAL.toString()) ? json
                .getString(Keys.RVAL.toString()) : null;
    }

    public HelloAppRequest(String name, String text, IntegerPacketType type) {
        this(name, text, 0, (int) (Math.random() * Integer.MAX_VALUE), type);
    }

    public HelloAppRequest(String text, HelloAppRequest req) {
        this(req.name, text, req.epoch, req.id, HelloAppRequest.PacketType
                .getPacketType(req.type));
    }

    private String response = null;

    @Override
    public ClientRequest getResponse() {
        if (this.response != null)
            return new HelloAppRequest(this.response, this);
        else
            return null;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PacketType.getPacketType(this.type);
    }

    @Override
    public String getServiceName() {
        return name;
    }

    @Override
    public long getRequestID() {
        return 0;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(HelloAppRequest.Keys.NAME.toString(), this.name);
        json.put(HelloAppRequest.Keys.EPOCH.toString(), this.epoch);
        json.put(HelloAppRequest.Keys.ID.toString(), this.id);
        json.put(HelloAppRequest.Keys.TEXT.toString(), this.text);
        json.putOpt(HelloAppRequest.Keys.RVAL.toString(), this.response);
        return json;
    }

    public String getText() {
        return this.text;
    }

    @Override
    public int getEpochNumber() {
        return this.epoch;
    }

    @Override
    public boolean isStop() {
        return false;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
