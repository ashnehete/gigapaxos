/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.gigapaxos.paxospackets;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.logging.Level;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.Summarizable;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.Config;

/**
 * 
 * @author arun
 *
 *         The parent class for all paxos packet types. Every paxos packet has a
 *         minimum of three fields: packet type, paxos group ID, version.
 */
public abstract class PaxosPacket extends JSONPacket implements Summarizable {

	/**
	 * Keys used in json packets.
	 */
	public static enum Keys {
		/**
		 * JSON key for packet type field in JSON representation.
		 */
		PT,
		/**
		 * JSON key for paxosID field.
		 */
		ID,
		/**
		 * JSON key for paxos version (or epoch number) field.
		 */
		V,
		/**
		 * slot
		 */
		S,
		/**
		 * median checkpointed slot; used for GC
		 */
		GC_S,
		/**
		 * accepted pvalues from lower ballots
		 */
		ACC_MAP,
		/**
		 * min slot in prepare reply
		 */
		PREPLY_MIN,
		/**
		 * whether recovery mode packet
		 */
		RCVRY,
		/**
		 * last checkpointed slot; used for GC
		 */
		CP_S,
		/**
		 * checkpoint state
		 */
		STATE,
		/**
		 * max decision slot; used to sync missing decisions
		 */
		MAX_S,
		/**
		 * max decision slot; used to sync missing decisions
		 */
		MIN_S,
		/**
		 * missing decisions
		 */
		MISS,
		/**
		 * first slot being prepared
		 */
		PREP_MIN,
		/**
		 * large checkpoint option?
		 */
		BIG_CP,
		/**
		 * sync mode; used by pause deactivator
		 */
		SYNCM,

		/**
		 * Set of slots used in batched commits and accept replies.
		 */
		SLOTS,

		/**
		 * Slot digest map used in batched accepts.
		 */
		S_DIGS,

		/**
		 * Slot requestID map used in batched accepts.
		 */
		S_QIDS,

		/**
		 * Slot batch size.
		 */
		S_BS,

		/**
		 * 
		 */
		NO_COALESCE,

		/**
		 * To batch general paxos packets.
		 */
		PP,

		/**
		 * Used by accept reply to request undigested accept.
		 */
		NACK,
	}

	/**
	 * These are keys involving NodeIDType that need to be "fixed" by Messenger
	 * just before sending and by PaxosPacketDemultiplexer just after being
	 * received. The former fix involves a int->NodeIDType->String conversion
	 * and the latter involves a String->NodeIDType->int conversion. The former
	 * requires IntegerMap<NodeIDType> and the latter requires
	 * Stringifiable<NodeIDTpe>.
	 */
	public static enum NodeIDKeys {
		/**
		 * 
		 */
		SNDR,
		/**
		 * Ballot.
		 */
		B,
		/**
		 * 
		 */
		ACCPTR,
		/**
		 * 
		 */
		GROUP,
		/**
		 * Entry replica integer ID.
		 */
		E
	}

	/* Every PaxosPacket has a minimum of the following three fields. The fields
	 * paxosID and version are preserved across inheritances, e.g.,
	 * ProposalPacket gets them from the corresponding RequestPacket it is
	 * extending. */
	protected PaxosPacketType packetType;
	protected String paxosID = null;
	protected int version = -1;

	/**
	 * The paxos packet type class.
	 */
	public enum PaxosPacketType implements IntegerPacketType {

		/**
		 * 
		 */
		REQUEST("REQUEST", 1),
		/**
		 * 
		 */
		PREPARE("PREPARE", 2),
		/**
		 * 
		 */
		ACCEPT("ACCEPT", 3),
		/**
		 * 
		 */
		RESEND_ACCEPT("RESEND_ACCEPT", 4),
		/**
		 * 
		 */
		PROPOSAL("PROPOSAL", 5),
		/**
		 * 
		 */
		DECISION("DECISION", 6),
		/**
		 * 
		 */
		PREPARE_REPLY("PREPARE_REPLY", 7),
		/**
		 * 
		 */
		ACCEPT_REPLY("ACCEPT_REPLY", 8),
		/**
		 * 
		 */
		FAILURE_DETECT("FAILURE_DETECT", 9),
		/**
		 * 
		 */
		PREEMPTED("PREEMPTED", 13),
		/**
		 * 
		 */
		CHECKPOINT_STATE("CHECKPOINT_STATE", 21),
		/**
		 * 
		 */
		CHECKPOINT_REQUEST("CHECKPOINT_REQUEST", 23),
		/**
		 * Request to retrieve missing decisions. A replica sends out this
		 * request when it has received a decision with slot number n+K but not
		 * n for a sufficiently large K. The value of K is generally a constant
		 * fixed at initialization time to a value based on the expected, normal
		 * length of the outstanding request pipeline. However, a paxos instance
		 * additionally tries to sync more aggressively at creation time, i.e.,
		 * the effective K is small, e.g., 1, because it is more likely that
		 * different instances got created at different times and some instances
		 * missed some decisions but the gap is much smaller than the default K.
		 */
		SYNC_DECISIONS_REQUEST("SYNC_DECISIONS", 32),
		/**
		 * 
		 */
		FIND_REPLICA_GROUP("FIND_REPLICA_GROUP", 33),

		/**
		 * 
		 */
		BATCHED_ACCEPT_REPLY("BATCHED_ACCEPT_REPLY", 34),

		/**
		 * 
		 */
		BATCHED_COMMIT("BATCHED_COMMIT", 35),

		/**
		 * 
		 */
		BATCHED_ACCEPT("BATCHED_ACCEPT", 36),

		/**
		 * 
		 */
		BATCHED_PAXOS_PACKET("BATCHED_PACKET", 37),

		/**
		 * 
		 */
		PAXOS_PACKET("PAXOS_PACKET", 90),
		/**
		 * 
		 */
		NO_TYPE("NO_TYPE", 9999);

		private static HashMap<String, PaxosPacketType> labels = new HashMap<String, PaxosPacketType>();
		private static HashMap<Integer, PaxosPacketType> numbers = new HashMap<Integer, PaxosPacketType>();

		private final String label;
		private final int number;

		PaxosPacketType(String s, int t) {
			this.label = s;
			this.number = t;
		}

		/************** BEGIN static code block to ensure correct initialization **************/
		static {
			for (PaxosPacketType type : PaxosPacketType.values()) {
				if (!PaxosPacketType.labels.containsKey(type.label)
						&& !PaxosPacketType.numbers.containsKey(type.number)) {
					PaxosPacketType.labels.put(type.label, type);
					PaxosPacketType.numbers.put(type.number, type);
				} else {
					assert (false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException(
							"Duplicate or inconsistent enum type");
				}
			}
		}

		/**************** END static code block to ensure correct initialization **************/

		public int getInt() {
			return number;
		}

		/**
		 * @return String label
		 */
		public String getLabel() {
			return label;
		}

		public String toString() {
			return getLabel();
		}

		/**
		 * @param type
		 * @return Integer type
		 */
		public static PaxosPacketType getPaxosPacketType(int type) {
			return PaxosPacketType.numbers.get(type);
		}

		/**
		 * @param type
		 * @return PaxosPacketType type
		 */
		public static PaxosPacketType getPaxosPacketType(String type) {
			return PaxosPacketType.labels.get(type);
		}
	}

	/**
	 * @param json
	 * @return PaxosPacketType type
	 * @throws JSONException
	 */
	public static PaxosPacketType getPaxosPacketType(JSONObject json)
			throws JSONException {
		return PaxosPacketType.getPaxosPacketType(json
				.getInt(PaxosPacket.Keys.PT.toString()));
	}

	/**
	 * @param json
	 * @return PaxosPacketType type
	 * @throws JSONException
	 */
	public static PaxosPacketType getPaxosPacketType(
			net.minidev.json.JSONObject json) throws JSONException {
		assert (json != null);
		if (json.get(PaxosPacket.Keys.PT.toString()) != null)
			return PaxosPacketType.getPaxosPacketType((Integer) json
					.get(PaxosPacket.Keys.PT.toString()));
		else
			return null;
	}

	protected abstract JSONObject toJSONObjectImpl() throws JSONException;

	protected net.minidev.json.JSONObject
	toJSONSmartImpl()
			throws JSONException {
		return null;
	}

	/* PaxosPacket has no no-arg constructor for a good reason. All classes
	 * extending PaxosPacket must in their constructors invoke super(JSONObject)
	 * or super(PaxosPacket) as a PaxosPacket is typically created only in one
	 * of those two ways. If a PaxosPacket is being created from nothing, the
	 * child should explicitly acknowledge that fact by invoking
	 * super((PaxosPacket)null), thereby acknowledging that paxosID is not set
	 * and is not meant to be set. Otherwise it is all too easy to forget to
	 * stamp a paxosID into a PaxosPacket. This is problematic when we create a
	 * new PaxosPacket internally and consume it internally within a paxos
	 * instance. A PaxosPacket that is coming in and, by consequence, any
	 * PaxosPacket that has been sent out of a paxos instance will have a good
	 * paxosID, so we don't need to worry about those. */
	protected PaxosPacket(PaxosPacket pkt) {
		super(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		if (pkt != null) {
			this.paxosID = pkt.paxosID;
			this.version = pkt.version;
		}
	}

	// directly supply the two fields
	protected PaxosPacket(String paxosID, int version) {
		super(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		this.paxosID = paxosID;
		this.version = version;
	}

	protected PaxosPacket(JSONObject json) throws JSONException {
		super(json);
		if (json != null) {
			if (json.has(PaxosPacket.Keys.ID.toString()))
				this.paxosID = json.getString(PaxosPacket.Keys.ID.toString());
			if (json.has(PaxosPacket.Keys.V.toString()))
				this.version = json.getInt(PaxosPacket.Keys.V.toString());
		}
	}

	// testing
	protected PaxosPacket(net.minidev.json.JSONObject json)
			throws JSONException {
		super(PaxosPacket.PaxosPacketType.getPaxosPacketType((Integer) json
				.get(JSONPacket.PACKET_TYPE)));
		if (json != null) {
			if (json.containsKey(PaxosPacket.Keys.ID.toString()))
				this.paxosID = (String) json
						.get(PaxosPacket.Keys.ID.toString());
			if (json.containsKey(PaxosPacket.Keys.V.toString()))
				this.version = (Integer) json
						.get(PaxosPacket.Keys.V.toString());
		}
	}

	protected static String CHARSET = "ISO-8859-1";
	protected static final boolean BYTEIFICATION = Config
			.getGlobalBoolean(PC.BYTEIFICATION);

	protected PaxosPacket(ByteBuffer bbuf) throws UnsupportedEncodingException,
			UnknownHostException {
		super(PaxosPacketType.PAXOS_PACKET);

		bbuf.getInt(); // packet type
		this.packetType = PaxosPacketType.getPaxosPacketType(bbuf.getInt());
		this.version = bbuf.getInt();
		byte paxosIDLength = bbuf.get();
		byte[] paxosIDBytes = new byte[paxosIDLength];
		bbuf.get(paxosIDBytes);
		this.paxosID = paxosIDBytes.length > 0 ? new String(paxosIDBytes,
				CHARSET) : null;
		int exactLength = (SIZEOF_PAXOSPACKET_FIXED + paxosIDBytes.length);
		assert (bbuf.position() == exactLength);
	}

	protected static final int SIZEOF_PAXOSPACKET_FIXED = 4 + 4 + 4 + 1;

	protected ByteBuffer toBytes(ByteBuffer bbuf)
			throws UnsupportedEncodingException {
		// paxospacket stuff
		bbuf.putInt(PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt()); // type
		bbuf.putInt(this.packetType.getInt()); // paxos type
		bbuf.putInt(this.version);
		// paxosID length followed by paxosID
		byte[] paxosIDBytes = this.paxosID != null ? this.paxosID
				.getBytes(CHARSET) : new byte[0];
		bbuf.put((byte) paxosIDBytes.length);
		bbuf.put(paxosIDBytes);
		assert (bbuf.position() == SIZEOF_PAXOSPACKET_FIXED
				+ paxosIDBytes.length) : bbuf.position() + " != "
				+ SIZEOF_PAXOSPACKET_FIXED + paxosIDBytes.length;
		return bbuf;
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		// tells Packet that this is a PaxosPacket
		JSONPacket.putPacketType(json,
				PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt());
		// the specific type of PaxosPacket
		json.put(PaxosPacket.Keys.PT.toString(), this.packetType.getInt());
		json.put(PaxosPacket.Keys.ID.toString(), this.paxosID);
		json.put(PaxosPacket.Keys.V.toString(), this.version);

		// copy over child fields
		JSONObject child = toJSONObjectImpl();
		for (String name : Util.getNames(child))
			json.put(name, child.get(name));

		return json;
	}

	/**
	 * @return JSONObject representation for {@code this}.
	 * @throws JSONException
	 */
	public net.minidev.json.JSONObject toJSONSmart() throws JSONException {
		net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
		// tells Packet that this is a PaxosPacket
		json.put(PaxosPacket.PACKET_TYPE,
				PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt());
		// the specific type of PaxosPacket
		json.put(PaxosPacket.Keys.PT.toString(), this.packetType.getInt());
		json.put(PaxosPacket.Keys.ID.toString(), this.paxosID);
		json.put(PaxosPacket.Keys.V.toString(), this.version);

		// copy over child fields
		net.minidev.json.JSONObject child = toJSONSmartImpl();
		if (child != null) {
			for (String name : (child).keySet())
				json.put(name, child.get(name));
		} else
			return null;

		return json;
	}

	/**
	 * @return Type
	 */
	public PaxosPacketType getType() {
		return this.packetType;
	}

	/**
	 * @param pid
	 * @param v
	 * @return {@code this}.
	 */
	public PaxosPacket putPaxosID(String pid, int v) {
		this.paxosID = pid;
		this.version = v;
		return this;
	}

	/**
	 * @return Paxos ID
	 */
	public String getPaxosID() {
		return this.paxosID;
	}

	/**
	 * @return Integer version (or epoch number)
	 */
	public int getVersion() {
		return this.version;
	}

	interface GetType {
		public Class<?> getType();
	}

	protected static boolean isStatic(Field field) {
		String[] tokens = field.toString().split("\\s+");
		for (String token : tokens)
			if (token.equals("static"))
				return true;
		return false;
	}
	
	private static final boolean STRICT_ADDRESS_CHECKS = Config.getGlobalBoolean(PC.STRICT_ADDRESS_CHECKS);

	// to prevent unintentional field sequence modifications
	static void checkFields(Class<?> clazz, GetType[] expFields) {
		if(!STRICT_ADDRESS_CHECKS) return;
		Field[] fields = clazz.getDeclaredFields();
		boolean print = PaxosConfig.getLogger()
				.isLoggable(Level.FINE);
		if (print)
			System.out.println("Checking fields for " + clazz);
		for (int i = 0, j = 0; i < fields.length; j += (isStatic(fields[i]) ? 0
				: 1), i++) {
			if (isStatic(fields[i]))
				continue;
			if (j >= expFields.length)
				break;
			if (print)
				System.out.println("    " + fields[j]);
			boolean match = true;
			String field = fields[i].getName();
			Class<?> type = fields[i].getType();
			match = match && field.equals(expFields[j].toString())
					&& type.equals(expFields[j].getType());
			if (!match)
				throw new RuntimeException("Field " + i + " = " + type + " "
						+ field + " does not match expected "
						+ expFields[j].getType() + " "
						+ expFields[j].toString());
		}
	}

	/**
	 * @return Paxos ID and version as a single string.
	 */
	public String getPaxosIDVersion() {
		return this.paxosID + ":" + this.version;
	}

	/**
	 * @param json
	 * @return True if packet generated during recovery mode.
	 * @throws JSONException
	 */
	public static boolean isRecovery(JSONObject json) throws JSONException {
		return json.optBoolean(PaxosPacket.Keys.RCVRY.toString());
	}

	/**
	 * Only prepare, accept, and decision can be recovery packets.
	 * 
	 * @param pp
	 * @return True if {@code pp} is a recovery packet.
	 */
	public static boolean isRecovery(PaxosPacket pp) {
		switch (pp.getType()) {
		case ACCEPT:
		case DECISION:
			return ((PValuePacket) pp).isRecovery();
		case PREPARE:
			return ((PreparePacket) pp).isRecovery();
		default:
			break;
		}
		return false;
	}

	@Override
	public String toString() {
		try {
			// for the types below, we use json-smart
			assert (this.packetType != PaxosPacketType.ACCEPT
					&& this.packetType != PaxosPacketType.DECISION
					&& this.packetType != PaxosPacketType.REQUEST && this.packetType != PaxosPacketType.BATCHED_PAXOS_PACKET);
			return this.toJSONObject().toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @return {@code this} as a singleton array.
	 */
	public PaxosPacket[] toSingletonArray() {
		PaxosPacket[] ppArray = { this };
		return ppArray;
	}

	protected abstract String getSummaryString();

	/**
	 * @return Object for pretty printing.
	 */
	public Object getSummary() {
		return getSummary(true);
	}

	/**
	 * @param create
	 * @return Object for pretty printing.
	 */
	public Object getSummary(boolean create) {
		if (!create)
			return null;
		return new Object() {
			public String toString() {
				return getPaxosID() + ":" + getVersion() + ":" + getType()
						+ ":[" + getSummaryString() + "]";
			}
		};
	}

	/************* Type-specific methods below *******************/
	/**
	 * @param packet
	 * @return PaxosPacket marked as recovery mode.
	 */
	public static PaxosPacket markRecovered(PaxosPacket packet) {
		PaxosPacket.PaxosPacketType type = packet.getType();
		switch (type) {
		case PREPARE:
			((PreparePacket) packet).setRecovery();
			break;
		case ACCEPT:
			((AcceptPacket) packet).setRecovery();
			break;
		case DECISION:
			((PValuePacket) packet).setRecovery();
			break;
		default:
			break;
		}
		return packet;
	}

	/**
	 * Returns a PaxosPacket if parseable from a string.
	 * 
	 * @param msg
	 * @return Paxos packet from string.
	 * 
	 * @throws JSONException
	 */
	public static PaxosPacket getPaxosPacket(String msg) throws JSONException {
		JSONObject jsonMsg = new JSONObject(msg);
		return getPaxosPacket(jsonMsg);
	}

	/**
	 * @param jsonMsg
	 * @return PaxosPacket from JSON.
	 * @throws JSONException
	 */
	public static PaxosPacket getPaxosPacket(JSONObject jsonMsg)
			throws JSONException {
		PaxosPacket paxosPacket = null;
		PaxosPacketType type = PaxosPacket.getPaxosPacketType(jsonMsg);
		switch (type) {
		case PREPARE:
			paxosPacket = new PreparePacket(jsonMsg);
			break;
		case ACCEPT:
			paxosPacket = new AcceptPacket(jsonMsg);
			break;
		case DECISION:
			paxosPacket = new PValuePacket(jsonMsg);
			break;
		default:
			assert (false);
		}
		return paxosPacket;

	}

	/**
	 * @param bytes
	 * @return True if first 8 bytes correspond to a legitimate PaxosPacketType.
	 */
	public static boolean isParseable(byte[] bytes) {
		ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, 8);
		return bbuf.getInt() == PaxosPacketType.PAXOS_PACKET.getInt()
				&& PaxosPacketType.getPaxosPacketType(bbuf.getInt()) != null;
	}

	/**
	 * @param bytes
	 * @return PaxosPacketType if parseable as bytes.
	 */
	public static PaxosPacketType getType(byte[] bytes) {
		PaxosPacketType type = null;
		ByteBuffer bbuf = ByteBuffer.wrap(bytes, 0, 8);
		return bbuf.getInt() == PaxosPacketType.PAXOS_PACKET.getInt()
				&& (type = PaxosPacketType.getPaxosPacketType(bbuf.getInt())) != null ? type
				: null;
	}

	/************* End of type-specific methods *******************/
}
