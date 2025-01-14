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
package edu.umass.cs.gigapaxos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.SummarizableRequest;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAccept;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAcceptReply;
import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.gigapaxos.paxospackets.PreparePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxospackets.SyncDecisionsPacket;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.gigapaxos.paxosutil.HotRestoreInfo;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.gigapaxos.paxosutil.LogMessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.MessagingTask;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.gigapaxos.paxosutil.PrepareReplyAssembler;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.gigapaxos.paxosutil.SlotBallotState;
import edu.umass.cs.gigapaxos.testing.TESTPaxosApp;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.nioutils.RTTEstimator;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Keyable;
import edu.umass.cs.utils.Pausable;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This class is the top-level paxos class per instance or paxos group
 *         on a machine. This class is "protected" as the only way to use it
 *         will be through the corresponding PaxosManager even if there is just
 *         one paxos application running on the machine.
 *         <p>
 * 
 *         This class delegates much of the interesting paxos actions to
 *         PaxosAcceptorState and PaxosCoordinator. It delegates all messaging
 *         to PaxosManager's PaxosMessenger. It is "managed", i.e., its paxos
 *         group is created and its incoming packets are demultiplexed, by its
 *         PaxosManager. It's logging is handled by an implementation of
 *         AbstractPaxosLogger.
 *         <p>
 * 
 *         The high-level organization is best reflected in handlePaxosMessage,
 *         a method that delegates processing to the acceptor or coordinator and
 *         gets back a messaging task, e.g., receiving a prepare message will
 *         probably result in a prepare-reply messaging task, and so on.
 *         <p>
 * 
 *         Space: An inactive PaxosInstanceStateMachine, i.e., whose
 *         corresponding application is currently not processing any requests,
 *         uses ~225B *total*. Here is the breakdown: PaxosInstanceStateMachine
 *         final fields: ~80B PaxosAcceptor: ~90B PaxosCoordinatorState: ~60B
 *         Even in an inactive paxos instance, the total *total* space is much
 *         more because of PaxosManager (that internally uses FailureDetection)
 *         etc., but all that state is not incurred per paxos application, just
 *         per machine. Thus, if we have S=10 machines and N=10M applications
 *         each using paxos with K=10 replicas one each at each machine, each
 *         machine has 10M PaxosInstanceStateMachine instances that will use
 *         about 2.25GB (10M*225B). The amount of space used by PaxosManager and
 *         others is small and depends only on S, not N or K.
 *         <p>
 * 
 *         When actively processing requests, the total space per paxos instance
 *         can easily go up to thousands of bytes. But we are unlikely to be
 *         processing requests across even hundreds of thousands of different
 *         applications simultaneously if each request finishes executing in
 *         under a second. For example, if a single server's execution
 *         throughput is 10K requests/sec and each request takes 100ms to finish
 *         executing (including paxos coordination), then the number of active
 *         *requests* at a machine is on average ~100K. The number of active
 *         paxos instances at that machine is at most the number of active
 *         requests at that machine.
 * 
 */
public class PaxosInstanceStateMachine implements Keyable<String>, Pausable {
	/* If false, the paxosID is represented as a byte[], so we must invoke
	 * getPaxosID() as infrequently as possible. */
	private static final boolean PAXOS_ID_AS_STRING = false;

	// must be >= 1, does not depend on anything else
	protected static final int INTER_CHECKPOINT_INTERVAL = Config
			.getGlobalInt(PaxosConfig.PC.CHECKPOINT_INTERVAL);// 100;

	// out-of-order-ness prompting synchronization, must be >=1
	protected static final int SYNC_THRESHOLD = 4 * INTER_CHECKPOINT_INTERVAL;

	// max decisions gap when reached will prompt checkpoint transfer
	protected static final int MAX_SYNC_DECISIONS_GAP = INTER_CHECKPOINT_INTERVAL;

	// minimum interval before another sync decisions request can be issued
	protected static final long MIN_RESYNC_DELAY = 1000;

	private static final boolean ENABLE_INSTRUMENTATION = Config
			.getGlobalBoolean(PC.ENABLE_INSTRUMENTATION);

	private static final boolean instrument() {
		return ENABLE_INSTRUMENTATION;
	}

	private static final boolean instrument(boolean flag) {
		return flag && ENABLE_INSTRUMENTATION;
	}

	private static final boolean instrument(int n) {
		return ENABLE_INSTRUMENTATION && Util.oneIn(n);
	}

	private static final void instrumentDelay(toLog field, long startTime) {
		if (field.log())
			DelayProfiler.updateDelay(field.toString(), startTime);
	}

	private static final void instrumentDelay(toLog field, long startTime, int n) {
		if (field.log())
			DelayProfiler.updateDelay(field.toString(), startTime, n);
	}

	private static enum SyncMode {
		DEFAULT_SYNC, FORCE_SYNC, SYNC_TO_PAUSE
	};

	/* Enabling this will slow down instance creation for null initialState as
	 * an initial checkpoint will still be made. It will make no difference if
	 * initialState is non-null as checkpointing non-null initial state is
	 * necessary for safety.
	 * 
	 * The default setting must be true. Not allowing null checkpoints can cause
	 * reconfiguration to stall as there is no way for the new epoch to
	 * distinguish between no previous epoch final state and null previous epoch
	 * final state. */
	protected static final boolean ENABLE_NULL_CHECKPOINT_STATE = true;

	/************ final Paxos state that is unchangeable after creation ***************/
	private final int[] groupMembers;
	// Object to allow easy testing across byte[] and String
	private final Object paxosID;
	private final int version;
	private final PaxosManager<?> paxosManager;
	// private final InterfaceReplicable clientRequestHandler;

	/************ Non-final paxos state that is changeable after creation *******************/
	// uses ~125B of empty space when not actively processing requests
	private PaxosAcceptor paxosState = null;
	// uses just a single pointer's worth of space unless I am a coordinator
	private PaxosCoordinator coordinator = null;
	/************ End of non-final paxos state ***********************************************/

	// static, so does not count towards space.
	private static final Logger log = (PaxosConfig.getLogger());

	PaxosInstanceStateMachine(String groupId, int version, int id,
			Set<Integer> gms, Replicable app, String initialState,
			PaxosManager<?> pm, final HotRestoreInfo hri, boolean missedBirthing) {

		/* Final assignments: A paxos instance is born with a paxosID, version
		 * this instance's node ID, the application request handler, the paxos
		 * manager, and the group members. */
		this.paxosID = PAXOS_ID_AS_STRING ? groupId : groupId.getBytes();
		this.version = version;
		// this.clientRequestHandler = app;
		this.paxosManager = pm;
		assert (gms != null && gms.size() > 0);
		Arrays.sort(this.groupMembers = Util.setToIntArray(gms));
		/**************** End of final assignments *******************/

		/* All non-final state is store in PaxosInstanceState (for acceptors) or
		 * in PaxosCoordinatorState (for coordinators) that inherits from
		 * PaxosInstanceState. */
		if (pm != null && hri == null)
			initiateRecovery(initialState, missedBirthing);
		else if ((hri != null) && hotRestore(hri)) {
			if (initialState != null) // batched creation
				// this.putInitialState(initialState);
				this.restore(initialState);
		} else if (pm == null)
			testingNoRecovery(); // used only for testing size
		assert (hri == null || initialState == null || hri.isCreateHRI()) : "Can not specify initial state for existing, paused paxos instance";
		incrInstanceCount(); // for instrumentation

		// log creation only if the number of instances is small
		log.log(((hri == null || initialState != null) && notManyInstances()) ? Level.INFO
				: Level.FINER,
				"{0} initialized paxos {1} {2} with members {3}; {4} {5} {6}",
				new Object[] {
						this.getNodeID(),
						(this.paxosState.getBallotCoordLog() == this.getMyID() ? "coordinator"
								: "acceptor"),
						this.getPaxosIDVersion(),
						Util.arrayOfIntToString(groupMembers),
						this.paxosState,
						this.coordinator,
						(initialState == null ? "{recovered_state=["
								+ Util.truncate(this.getCheckpointState(), 64, 64)
								: "{initial_state=[" + initialState)
								+ "]}" });
	}

	/**
	 * @return Version or epoch number corresponding to this reconfigurable
	 *         paxos instance.
	 */
	protected int getVersion() {
		return this.version;
	}

	// one of only two public methods
	public String getKey() {
		return this.getPaxosID();
	}

	public String toString() {
		return this.getNodeState();
	}

	protected String toStringLong() {
		return this.getNodeState() + this.paxosState
				+ (this.coordinator != null ? this.coordinator : "");
	}

	/**
	 * @return Paxos instance name concatenated with the version number.
	 */
	protected String getPaxosIDVersion() {
		return this.getPaxosID() + ":" + this.getVersion();
	}

	protected String getPaxosID() {
		return (paxosID instanceof String ? (String) paxosID : new String(
				(byte[]) paxosID));
	}

	protected int[] getMembers() {
		return this.groupMembers;
	}

	protected String getNodeID() {
		return this.paxosManager != null ? this.paxosManager.intToString(this
				.getMyID()) : "" + getMyID();
	}

	protected Replicable getApp() {
		return this.paxosManager.getApp(this.getPaxosID()); // this.clientRequestHandler;
	}

	protected PaxosManager<?> getPaxosManager() {
		return this.paxosManager;
	}

	protected int getMyID() {
		return (this.paxosManager != null ? this.paxosManager.getMyID() : -1);
	}

	/**
	 * isStopped()==true means that this paxos instance is dead and completely
	 * harmless (even if the underlying object has not been garbage collected by
	 * the JVM. In particular, it can NOT make the app execute requests or send
	 * out paxos messages to the external world.
	 * 
	 * @return Whether this paxos instance has been stopped.
	 */
	protected boolean isStopped() {
		return this.paxosState.isStopped();
	}

	/**
	 * Forces a synchronization wait. PaxosManager needs this to ensure that an
	 * ongoing stop is fully executed.
	 * 
	 * @return True.
	 */
	protected synchronized boolean synchronizedNoop() {
		return true;
	}

	// not synchronized as coordinator can die anytime anyway
	protected boolean forceStop() {
		if (!this.paxosState.isStopped())
			decrInstanceCount(); // for instrumentation
		PaxosCoordinator.forceStop(this.coordinator);
		this.coordinator = null;
		this.paxosState.forceStop(); //
		return true;
	}

	private boolean nullCheckpointStateEnabled() {
		return this.paxosManager.isNullCheckpointStateEnabled();
	}

	// removes all database and app state and can not be recovered anymore
	protected boolean kill(boolean clean) {
		// paxosState must be typically already stopped here
		this.forceStop();
		if (clean // clean kill implies reset app state
				&& this.nullifyAppState(this.getPaxosID(), null)
				// and remove database state
				&& AbstractPaxosLogger.kill(this.paxosManager.getPaxosLogger(),
						getPaxosID(), this.getVersion()))
			// paxos instance is "lost" now
			log.log(Level.FINE, "Paxos instance {0} cleanly terminated.",
					new Object[] { this });
		else
			// unclean "crash"
			log.severe(this
					+ " crashing paxos instance "
					+ getPaxosIDVersion()
					+ " likely because of an error while executing an application request. "
					+ "A paxos instance for "
					+ getPaxosIDVersion()
					+ " or a higher version must either be explicitly (re-)created "
					+ "or this \"crashed\" instance will recover safely upon a reboot.");
		return true;
	}

	private boolean nullifyAppState(String paxosID, String state) {
		for (int i = 0; !this.restore(null); i++)
			if (waitRetry(RETRY_TIMEOUT) && i < RETRY_LIMIT)
				log.warning(this
						+ " unable to delete application state; retrying");
			else
				throw new RuntimeException(getNodeID()
						+ " unable to delete " + this.getPaxosIDVersion());
		return true;
	}

	private static final long RETRY_TIMEOUT = Config
			.getGlobalLong(PC.HANDLE_REQUEST_RETRY_INTERVAL);
	private static final int RETRY_LIMIT = Config
			.getGlobalInt(PC.HANDLE_REQUEST_RETRY_LIMIT);

	private static final boolean waitRetry(long timeout) {
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	protected void setActive() {
		this.paxosState.setActive();
		/* If acceptor slot is 0 and my coordinator ballot number is 0, this
		 is the initial default coordinator, so we need to mark it as active2
		  in order to obviate running for coordinator for the very first
		  request.
		*/
		if(this.coordinator!=null && this.coordinator.getBallot().ballotNumber==0 && this.paxosState.getSlot()==0)
			this.paxosState.setActive2();
	}

	protected boolean isActive() {
		return this.paxosState.isActive();
	}

	private String getCheckpointState() {
		SlotBallotState sbs = this.paxosManager != null ? this.paxosManager
				.getPaxosLogger()
				.getSlotBallotState(getPaxosID(), getVersion()) : null;
		return sbs != null ? sbs.state : null;
	}

	/**
	 * This is the main entry point into this class and is used by
	 * {@link PaxosManager} to supply incoming packets.
	 * 
	 * @param obj
	 *            JSONObject or RequestPacket.
	 * @throws JSONException
	 */
	protected void handlePaxosMessage(PaxosPacket obj) throws JSONException {
		this.handlePaxosMessage(obj, SyncMode.DEFAULT_SYNC);
	}

	/**
	 * For legacy reasons, this method still accepts JSONObject in addition to
	 * PaxosPacket as the first argument.
	 * 
	 * @param pp
	 * @param mode
	 * @throws JSONException
	 */
	private void handlePaxosMessage(PaxosPacket pp, SyncMode mode)
			throws JSONException {
		long methodEntryTime = System.currentTimeMillis();
		assert (pp != null || !mode.equals(SyncMode.DEFAULT_SYNC));

		PaxosPacket.PaxosPacketType msgType = pp != null ? pp.getType()
				: PaxosPacket.PaxosPacketType.NO_TYPE;

		Level level = Level.FINEST;
		log.log(level,
				"{0} starting handlePaxosMessage{3}({1}) {2}",
				new Object[] {
						this,
						pp != null ? pp.getSummary(log.isLoggable(level))
								: msgType,
						mode != SyncMode.DEFAULT_SYNC ? mode : "",
						pp!=null && log.isLoggable(level)? pp.hashCode()+"":"" });

		if (pp != null && pp.getVersion() != this.getVersion()) {
			log.log(Level.INFO,
					"{0} version inconsistent with {1}; returning",
					new Object[] { this,
							pp.getSummary(log.isLoggable(Level.INFO)) });
			return;
		}

		/* Note: Because incoming messages may be handled concurrently, some
		 * messages may continue to get processed for a little while after a
		 * stop has been executed and even after isStopped() is true (because
		 * isStopped() was false when those messages came in here). But that is
		 * okay coz these messages can not spawn unsafe outgoing messages (as
		 * messaging is turned off for all but DECISION or CHECKPOINT_STATE
		 * packets) and can not change any disk state. */
		if (this.paxosState.isStopped()) {
			log.log(Level.INFO, "{0} stopped; dropping {1}; returning", new Object[] {
					this, pp!=null ? pp.getSummary() : null });
			return;
		}

		// recovery means we won't send any replies
		boolean recovery = pp != null ? PaxosPacket.isRecovery(pp) : false;
		/* The reason we should not process regular messages until this instance
		 * has rolled forward is that it might respond to a prepare with a list
		 * of accepts fetched from disk that may be inconsistent with its
		 * acceptor state. */
		if (!this.paxosManager.hasRecovered(this) && !recovery && pp!=null)
			return; // only process recovery message during rollForward

		boolean isPoke = msgType.equals(PaxosPacketType.NO_TYPE);
		if (!isPoke)
			this.markActive();

		MessagingTask[] mtasks = new MessagingTask[3];
		/* Check for coordinator'ing upon *every* message except poke messages.
		 * Pokes are primarily for sync'ing decisions and could be also used to
		 * resend accepts. There is little reason to send prepares proactively
		 * if no new activity is happening. */
		mtasks[0] = (!recovery ?
		// check run for coordinator if not active
		(!PaxosCoordinator.isActive(this.coordinator)
		// ignore pokes unless not caught up
		&& (!isPoke || !PaxosCoordinator.caughtUp(this.coordinator))) ? checkRunForCoordinator()
				// else reissue long waiting accepts
				: this.pokeLocalCoordinator()
				// neither during recovery
				: null);

		MessagingTask mtask = null;
		MessagingTask[] batchedTasks = null;

		switch (msgType) {
		case REQUEST:
			batchedTasks = handleRequest((RequestPacket) pp);
			// send RequestPacket to current coordinator
			break;
		// replica --> coordinator
		case PROPOSAL:
			batchedTasks = handleProposal((ProposalPacket) pp);
			// unicast ProposalPacket to coordinator or multicast AcceptPacket
			break;
		// coordinator --> replica
		case DECISION:
			mtask = handleCommittedRequest((PValuePacket) pp);
			// send nothing, but log decision
			break;
		case PREEMPTED:
			// do nothing
			break;
		case BATCHED_COMMIT:
			mtask = handleBatchedCommit((BatchedCommit) pp);
			// send nothing, but log decision
			break;
		// coordinator --> replica
		case PREPARE:
			mtask = handlePrepare((PreparePacket) pp);
			// send PreparePacket prepare reply to coordinator
			break;
		// replica --> coordinator
		case PREPARE_REPLY:
			mtask = handlePrepareReply((PrepareReplyPacket) pp);
			// send AcceptPacket[] to all
			break;
		// coordinator --> replica
		case ACCEPT:
			batchedTasks = handleAccept((AcceptPacket) pp);
			// send AcceptReplyPacket to coordinator
			break;
		// replica --> coordinator
		case ACCEPT_REPLY:
			mtask = handleAcceptReply((AcceptReplyPacket) pp);
			// send PValuePacket decision to all
			break;
		case BATCHED_ACCEPT_REPLY:
			batchedTasks = handleBatchedAcceptReply((BatchedAcceptReply) pp);
			// send PValuePacket decisions to all
			break;
		case BATCHED_ACCEPT:
			batchedTasks = handleBatchedAccept((BatchedAccept) pp);
			break;
		case SYNC_DECISIONS_REQUEST:
			mtask = handleSyncDecisionsPacket((SyncDecisionsPacket) pp);
			// send SynchronizeReplyPacket to sender
			break;
		case CHECKPOINT_STATE:
			mtask = handleCheckpoint((StatePacket) pp);
			break;
		case NO_TYPE: // not a real packet
			// sync if needed on poke
			mtasks[0] = (mtasks[0] != null) ? mtasks[0] : this
					.syncLongDecisionGaps(null, mode);
			break;
		default:
			assert (false) : "Paxos instance received an unrecognizable packet: "
					+ (pp.getSummary());
		}
		mtasks[1] = mtask;

		// special case for method returning array of messaging tasks
		if (batchedTasks != null) {
			// mtasks[1] = batchedTasks[0];
			// mtasks[2] = batchedTasks[1];
			mtasks = MessagingTask.combine(mtasks, batchedTasks);
		}

		instrumentDelay(toLog.handlePaxosMessage, methodEntryTime);

		this.checkIfTrapped(pp, mtasks[1]); // just to print a warning
		if (!recovery) {
			this.sendMessagingTask(mtasks);
		}
		
		level = Level.FINEST;
		if (pp != null)
			log.log(level, "{0} finished handlePaxosMessage{2}({1})",
					new Object[] { this, pp.getSummary(log.isLoggable(level)),
							log.isLoggable(level) ? pp.hashCode()+"":"" });
	}

	/************** Start of private methods ****************/

	/* Invoked both when a paxos instance is first created and when it recovers
	 * after a crash. It is all the same as far as the paxos instance is
	 * concerned (provided we ensure that the app state after executing the
	 * first request (slot 0) is checkpointed, which we do). */
	private boolean initiateRecovery(String initialState, boolean missedBirthing) {
		String pid = this.getPaxosID();
		// only place where version is checked
		SlotBallotState slotBallot = this.paxosManager.getPaxosLogger()
				.getSlotBallotState(pid, this.getVersion());

		if (slotBallot != null) {
			log.log(Level.FINE, "{0} recovered state: {1}", new Object[] {
					this, (slotBallot != null ? slotBallot.state : "NULL") });
			// check membership
			if (!slotBallot.members.equals(this.paxosManager
					.getStringNodesFromIntArray(groupMembers)))
				throw new PaxosInstanceCreationException(
						"Paxos instance exists with a different replica group: "
								+ (slotBallot.members));
			// update app state
			if (!this.restore(slotBallot.state))
				throw new PaxosInstanceCreationException(
						"Unable to update app state with " + slotBallot.state);
		}

		this.coordinator = null;// new PaxosCoordinator(); // just a shell class
		// initial coordinator is assumed, not prepared
		if (slotBallot == null && roundRobinCoordinator(0) == this.getMyID())
			this.coordinator = PaxosCoordinator.createCoordinator(0,
					this.getMyID(), getMembers(), (initialState != null
							|| nullCheckpointStateEnabled() ? 1 : 0), true,
					this.getNodeID()); // slotBallot==null
		/* Note: We don't have to create coordinator state here. It will get
		 * created if needed when the first external (non-recovery) packet is
		 * received. But we create the very first coordinator here as otherwise
		 * it is possible that no coordinator gets elected as follows: the
		 * lowest ID node wakes up and either upon an external or self-poke
		 * message sends a prepare, but gets no responses because no other node
		 * is up yet. In this case, the other nodes when they boot up will not
		 * run for coordinator, and the lowest ID node will not resend its
		 * prepare if no more requests come, so the first request could be stuck
		 * in its pre-active queue for a long time. */

		// allow null state without null checkpoints just for memory testing
		if (slotBallot == null && initialState == null
				&& !this.paxosManager.isNullCheckpointStateEnabled()
				&& !Config.getGlobalBoolean(TC.MEMORY_TESTING))
			throw new PaxosInstanceCreationException(
					"A paxos instance with null initial state can be"
							+ " created only if null checkpoints are enabled");

		/* If this is a "missed-birthing" instance creation, we still set the
		 * acceptor nextSlot to 0 but don't checkpoint initialState. In fact,
		 * initialState better be null here in that case as we can't possibly
		 * have an initialState with missed birthing. */
		assert (!(missedBirthing && initialState != null));
		/* If it is possible for there to be no initial state checkpoint, under
		 * missed birthing, an acceptor may incorrectly report its gcSlot as -1,
		 * and if a majority do so (because that majority consists all of missed
		 * birthers), a coordinator may propose a proposal for slot 0 even
		 * though an initial state does exist, which would end up overwriting
		 * the initial state. So we can not support ambiguity in whether there
		 * is initial state or not. If we force initial state checkpoints (even
		 * null state checkpoints) to always exist, missed birthers can always
		 * set the initial gcSlot to 0. The exception and assert above imply the
		 * assertion below. */
		assert (!missedBirthing || this.paxosManager
				.isNullCheckpointStateEnabled());

		this.paxosState = new PaxosAcceptor(
				slotBallot != null ? slotBallot.ballotnum : 0,
				slotBallot != null ? slotBallot.coordinator : this
						.roundRobinCoordinator(0),
				slotBallot != null ? (slotBallot.slot + 1) : 0, null) {
			public String toString() {
				return PaxosAcceptor.class.getSimpleName() + ":" + PaxosInstanceStateMachine.this.getNodeID();
			}
		};
		if (slotBallot == null && !missedBirthing)
			this.putInitialState(initialState); // will set nextSlot to 1
		if (missedBirthing)
			this.paxosState.setGCSlotAfterPuttingInitialSlot();

		if (slotBallot == null)
			// TESTPaxosConfig.setRecovered(this.getMyID(), pid, true)
			;

		return true; // return value will be ignored
	}

	private boolean hotRestore(HotRestoreInfo hri) {
		// called from constructor only, hence assert
		assert (this.paxosState == null && this.coordinator == null);
		log.log(Level.FINE, "{0} hot restoring with {1}", new Object[] { this,
				hri });
		this.coordinator = hri.coordBallot != null
				&& hri.coordBallot.coordinatorID == getMyID() ? PaxosCoordinator
				.hotRestore(this.coordinator, hri) : null;
		this.paxosState = new PaxosAcceptor(hri.accBallot.ballotNumber,
				hri.accBallot.coordinatorID, hri.accSlot, hri);
		this.paxosState.setActive(); // no recovery
		this.markActive(); // to prevent immediate re-pause
		return true;
	}

	private boolean putInitialState(String initialState) {
		if (this.getPaxosManager() == null
				|| (initialState == null && !nullCheckpointStateEnabled()))
			return false;
		this.handleCheckpoint(new StatePacket(initialBallot(), 0, initialState));
		this.paxosState.setGCSlotAfterPuttingInitialSlot();
		return true;
	}

	private Ballot initialBallot() {
		return new Ballot(0, this.roundRobinCoordinator(0));
	}

	/* The one method for all message sending. Protected coz the logger also
	 * calls this. */
	protected void sendMessagingTask(MessagingTask mtask) {
		if (mtask == null || mtask.isEmpty())
			return;
		if (this.paxosState != null
				&& this.paxosState.isStopped()
				&& !mtask.msgs[0].getType().equals(PaxosPacketType.DECISION)
				&& !mtask.msgs[0].getType().equals(
						PaxosPacketType.CHECKPOINT_STATE))
			return;
		// if (TESTPaxosConfig.isCrashed(this.getMyID()))return;

		log.log(Level.FINEST, "{0} sending: {1}", new Object[] { this, mtask });
		mtask.putPaxosIDVersion(this.getPaxosID(), this.getVersion());
		try {
			// assert(this.paxosState.isActive());
			paxosManager.send(mtask);
		} catch (IOException ioe) {
			log.severe(this + " encountered IOException while sending " + mtask);
			ioe.printStackTrace();
			/* We can't throw this exception upward because it will get sent all
			 * the way back up to PacketDemultiplexer whose incoming packet
			 * initiated this whole chain of events. It seems silly for
			 * PacketDemultiplexer to throw an IOException caused by the sends
			 * resulting from processing that packet. So we should handle this
			 * exception right here. But what should we do? We can ignore it as
			 * the network does not need to be reliable anyway. Revisit as
			 * needed. */
		} catch (JSONException je) {
			/* Same thing for other exceptions. Nothing useful to do here */
			log.severe(this + " encountered JSONException while sending "
					+ mtask);
			je.printStackTrace();
		}
	}

	private void sendMessagingTask(MessagingTask[] mtasks) throws JSONException {
		for (MessagingTask mtask : mtasks)
			this.sendMessagingTask(mtask);
	}

	// will send a noop message to self to force event-driven actions
	protected void poke(boolean forceSync) {
		try {
			SyncMode sync = forceSync ? SyncMode.FORCE_SYNC
					: SyncMode.SYNC_TO_PAUSE;
			log.log(Level.FINE, "{0} being poked {1}", new Object[] { this,
					sync });
			this.handlePaxosMessage(null, sync);
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}

	private static final boolean BATCHING_ENABLED = Config
			.getGlobalBoolean(PC.BATCHING_ENABLED);

	/* "Phase0" Event: Received a request from a client.
	 * 
	 * Action: Call handleProposal which will send the corresponding proposal
	 * to the current coordinator. */
	private MessagingTask[] handleRequest(RequestPacket request) {
		log.log(Level.FINE,
				"{0}{1}{2}",
				new Object[] { this, " Phase0/CLIENT_REQUEST: ",
						request.getSummary(log.isLoggable(Level.FINE)) });
		RequestInstrumenter.received(request, request.getClientID(),
				this.getMyID());

		if (!BATCHING_ENABLED
				|| request.getEntryReplica() == IntegerMap.NULL_INT_NODE
				|| request.isBroadcasted()) {
			this.paxosManager.incrOutstanding(request);
		}

		if (request.isBroadcasted()) {
			AcceptPacket accept = this.paxosManager.release(request);
			if (accept != null) {
				log.log(Level.FINE, "{0} released accept {1}", new Object[] {
						this, accept.getSummary(log.isLoggable(Level.FINE)) });
				return this.handleAccept(accept);
			}
		}

		// multicast to others if digests enabled
		MessagingTask mtask = (this.paxosManager.shouldDigest()
				&& request.getEntryReplica() == this.getMyID() && request
				.shouldBroadcast()) ? new MessagingTask(
				this.otherGroupMembers(), request
						.setDigest(
								request.getDigest(this.paxosManager
										.getMessageDigest())).setBroadcasted())
				: null;

		return MessagingTask.combine(mtask, handleProposal(request));
	}

	private static final boolean DIGEST_REQUESTS = Config
			.getGlobalBoolean(PC.DIGEST_REQUESTS);
	private static final boolean BATCHED_ACCEPTS = Config
			.getGlobalBoolean(PC.DIGEST_REQUESTS)
			&& !Config.getGlobalBoolean(PC.FLIP_BATCHED_ACCEPTS);

	/* "Phase0"->Phase2a: Event: Received a proposal [request, slot] from any
	 * node.
	 * 
	 * Action: If a non-coordinator node receives a proposal, send to the
	 * coordinator. Otherwise, propose it to acceptors with a good slot number
	 * (thereby initiating phase2a for this request).
	 * 
	 * Return: A send either to a coordinator of the proposal or to all replicas
	 * of the proposal with a good slot number. */
	private MessagingTask[] handleProposal(RequestPacket proposal) {
		assert (proposal.getEntryReplica() != IntegerMap.NULL_INT_NODE) : proposal;
		// could be multicast to all or unicast to coordinator
		MessagingTask[] mtasks = new MessagingTask[2];
		RequestInstrumenter.received(proposal, proposal.getForwarderID(),
				this.getMyID());
		if (PaxosCoordinator.exists(this.coordinator,
				this.paxosState.getBallot())) {
			// multicast ACCEPT to all
			proposal.addDebugInfoDeep("a");
			AcceptPacket multicastAccept = this.getPaxosManager()
					.getPreviouslyIssuedAccept(proposal);
			if (multicastAccept == null || !multicastAccept.ballot.equals(this.coordinator.getBallot()))
				this.getPaxosManager().setIssuedAccept(
						multicastAccept = PaxosCoordinator.propose(
								this.coordinator, this.groupMembers, proposal));
			if (multicastAccept != null) {
				assert (this.coordinator.getBallot().coordinatorID == getMyID() && multicastAccept.sender == getMyID());
				if (proposal.isBroadcasted())
					multicastAccept = this.paxosManager.digest(multicastAccept);
				mtasks[0] = multicastAccept != null ? new MessagingTask(
						this.groupMembers, multicastAccept) : null; // multicast
				RequestInstrumenter.sent(multicastAccept, this.getMyID(), -1);
				log.log(Level.FINER,
						"{0} issuing accept {1} ",
						new Object[] {
								this,
								multicastAccept.getSummary(log
										.isLoggable(Level.FINER)) });
			}
		} else if (!proposal.isBroadcasted()) { // else unicast to current
												// coordinator
			log.log(Level.FINER,
					"{0} is not the coordinator; forwarding to {1}: {2}",
					new Object[] { this, this.paxosManager.intToString(this.paxosState.getBallotCoordLog()),
							proposal.getSummary(log.isLoggable(Level.FINER)) });
			int coordinator = this.paxosState.getBallotCoord();

			mtasks[0] = new MessagingTask(
					this.paxosManager.isNodeUp(coordinator) ? coordinator
							// send to next coordinator if current seems dead
							: (coordinator = this.getNextCoordinator(
									this.paxosState.getBallot().ballotNumber + 1,
									groupMembers)),
					proposal.setForwarderID(this.getMyID())); // unicast
			if ((proposal.isPingPonging() || coordinator == this.getMyID())) {
				if (proposal.isPingPonging())
					log.warning(this + " jugglinging ping-ponging proposal: "
							+ proposal.getSummary() + " forwarded by "
							+ proposal.getForwarderID());
				Level level = Level.INFO;
				log.log(level,
						"{0} force running for coordinator; forwardCount={1}; debugInfo = {2}; coordinator={3}",
						new Object[] { this, proposal.getForwardCount(),
								proposal.getDebugInfo(log.isLoggable(level)),
								coordinator });
				if (proposal.getForwarderID() != this.getMyID())
					mtasks[1] = new MessagingTask(getMyID(), mtasks[0].msgs);
				mtasks[0] = this.checkRunForCoordinator(true);
			} else { // forwarding
				proposal.addDebugInfo("f", coordinator);
			}
		}
		return mtasks;
	}

	/* Phase1a Event: Received a prepare request for a ballot, i.e. that
	 * ballot's coordinator is acquiring proposing rights for all slot numbers
	 * (lowest uncommitted up to infinity)
	 * 
	 * Action: This node needs to check if it has accepted a higher numbered
	 * ballot already and if not, it can accept this ballot, thereby promising
	 * not to accept any lower ballots.
	 * 
	 * Return: Send prepare reply with proposal values previously accepted to
	 * the sender (the received ballot's coordinator). */
	private MessagingTask handlePrepare(PreparePacket prepare) {
		paxosManager.heardFrom(prepare.ballot.coordinatorID); // FD optimization

		Ballot prevBallot = this.paxosState.getBallot();
		PrepareReplyPacket prepareReply = this.paxosState.handlePrepare(
				prepare, this.paxosManager.getMyID());
		if (prepareReply == null)
			return null; // can happen only if acceptor is stopped
		if (prepare.isRecovery())
			return null; // no need to get accepted pvalues from disk during
							// recovery as networking is disabled anyway

		// may also need to look into disk if ACCEPTED_PROPOSALS_ON_DISK is true
		if (PaxosAcceptor.GET_ACCEPTED_PVALUES_FROM_DISK
		// no need to gather pvalues if NACKing anyway
				&& prepareReply.ballot.compareTo(prepare.ballot) == 0)
			prepareReply.accepted.putAll(this.paxosManager.getPaxosLogger()
					.getLoggedAccepts(this.getPaxosID(), this.getVersion(),
							prepare.firstUndecidedSlot));

		for (PValuePacket pvalue : prepareReply.accepted.values())
			// if I accepted a pvalue, my acceptor ballot must reflect it
			assert (this.paxosState.getBallot().compareTo(pvalue.ballot) >= 0) : this
					+ ":" + pvalue;

		log.log(Level.INFO,
				"{0} {1} {2} with {3}",
				new Object[] {
						this,
						prepareReply.ballot.compareTo(prepare.ballot) > 0 ? "preempting"
								: "acking", prepare.getSummary(),
						prepareReply.getSummary(log.isLoggable(Level.INFO)) });

		MessagingTask mtask = prevBallot.compareTo(prepareReply.ballot) < 0 ?
		// log only if not already logged (if my ballot got upgraded)
		new LogMessagingTask(prepare.ballot.coordinatorID,
		// ensures large prepare replies are fragmented
				PrepareReplyAssembler.fragment(prepareReply), prepare)
				// else just send prepareReply
				: new MessagingTask(prepare.ballot.coordinatorID,
						PrepareReplyAssembler.fragment(prepareReply));
		for (PaxosPacket pp : mtask.msgs)
			assert (((PrepareReplyPacket) pp).getLengthEstimate() < NIOTransport.MAX_PAYLOAD_SIZE) : Util
					.suicide(this
							+ " trying to return unfragmented prepare reply of size "
							+ ((PrepareReplyPacket) pp).getLengthEstimate()
							+ " : " + pp.getSummary() + "; prevBallot = "
							+ prevBallot);
		return mtask;
	}

	/* Phase1b Event: Received a reply to my ballot preparation request.
	 * 
	 * Action: If the reply contains a higher ballot, we must resign.
	 * Otherwise, if we acquired a majority with the receipt of this reply, send
	 * all previously accepted (but uncommitted) requests reported in the
	 * prepare replies, each in its highest reported ballot, to all replicas.
	 * These are the proposals that get carried over across a ballot change and
	 * must be re-proposed.
	 * 
	 * Return: A list of messages each of which has to be multicast (proposed)
	 * to all replicas. */
	private MessagingTask handlePrepareReply(PrepareReplyPacket prepareReply) {
		// necessary to defragment first for safety
		if ((prepareReply = PrepareReplyAssembler.processIncoming(prepareReply)) == null) {
			return null;
		}
		this.paxosManager.heardFrom(prepareReply.acceptor); // FD optimization,
		MessagingTask mtask = null;
		ArrayList<ProposalPacket> preActiveProposals = null;
		ArrayList<AcceptPacket> acceptList = null;

		if ((preActiveProposals = PaxosCoordinator.getPreActivesIfPreempted(
				this.coordinator, prepareReply, this.groupMembers)) != null) {
			log.log(Level.INFO,
					"{0} ({1}) election PREEMPTED by {2}",
					new Object[] { this,
							PaxosCoordinator.getBallotStr(this.coordinator),
							prepareReply.ballot });
			this.coordinator = null;
			if (!preActiveProposals.isEmpty())
				mtask = new MessagingTask(prepareReply.ballot.coordinatorID,
						(preActiveProposals.toArray(new PaxosPacket[0])));
		} else if ((acceptList = PaxosCoordinator.handlePrepareReply(
				this.coordinator, prepareReply, this.groupMembers)) != null
				&& !acceptList.isEmpty()) {
			mtask = new MessagingTask(this.groupMembers,
					((acceptList).toArray(new PaxosPacket[0])));
			// can't have previous accepts as just became coordinator
			for(AcceptPacket accept : acceptList)
				this.getPaxosManager().setIssuedAccept(accept);
			log.log(Level.INFO, "{0} elected coordinator; sending {1}",
					new Object[] { this, mtask });
		} else if(acceptList != null && this.coordinator.isActive())
			/* This case can happen when a node runs for coordinator and gets
			 * elected without being prompted by a client request, which can
			 * happen upon the receipt of any paxos packet and the presumed
			 * coordinator has been unresponsive for a while.
			 */
			log.log(Level.INFO, "{0} elected coordinator; no ACCEPTs to send",
					new Object[] { this});
		else
			log.log(Level.FINE, "{0} received prepare reply {1}", new Object[] {
					this, prepareReply.getSummary(log.isLoggable(Level.INFO)) });

		return mtask; // Could be unicast or multicast
	}

	private static final boolean GC_MAJORITY_EXECUTED = Config
			.getGlobalBoolean(PC.GC_MAJORITY_EXECUTED);

	/* Phase2a Event: Received an accept message for a proposal with some
	 * ballot.
	 * 
	 * Action: Send back current or updated ballot to the ballot's coordinator. */
	private static final boolean EXECUTE_UPON_ACCEPT = Config
			.getGlobalBoolean(PC.EXECUTE_UPON_ACCEPT);

	private MessagingTask[] handleAccept(AcceptPacket accept) {
		this.paxosManager.heardFrom(accept.ballot.coordinatorID); // FD
		RequestInstrumenter.received(accept, accept.sender, this.getMyID());

		// if(!accept.hasRequestValue())
		// DelayProfiler.updateCount("C_DIGESTED_ACCEPTS_RCVD",
		// accept.batchSize()+1);

		AcceptPacket copy = accept;
		if (DIGEST_REQUESTS && !accept.hasRequestValue()
				&& (accept = this.paxosManager.match(accept)) == null) {
			log.log(Level.FINE, "{0} received unmatched accept ", new Object[] {
					this, copy.getSummary(log.isLoggable(Level.FINE)) });
			// if(this.paxosState.getSlot() - copy.slot > 0)
			// DelayProfiler.updateCount("C_EXECD_ACCEPTS_RCVD",
			// copy.batchSize()+1);
			return new MessagingTask[0];
		} else
			log.log(Level.FINER, "{0} received matching accept for {1}", new Object[] {
					this, accept.getSummary() });

		// DelayProfiler.updateCount("C_ACCEPTS_RCVD", accept.batchSize()+1);
		assert (accept.hasRequestValue());

		if (instrument(10))
			DelayProfiler.updateMovAvg("#batched", accept.batchSize() + 1);
		if ((this.paxosState.getAccept(accept.slot) == null)
				&& (this.paxosState.getSlot() - accept.slot <= 0))
			this.paxosManager.incrOutstanding(accept.addDebugInfoDeep("a")); // stats

		if (EXECUTE_UPON_ACCEPT) { // only for testing
			PaxosInstanceStateMachine.execute(this, getPaxosManager(),
					this.getApp(), accept, false);
			if (Util.oneIn(10))
				log.log(Level.INFO, "{0}", new Object[]{DelayProfiler.getStats()});
			// return null;
		}

		// have acceptor handle accept
		Ballot ballot = null;
		PValuePacket prev = this.paxosState.getAccept(accept.slot);
		try {
			ballot = !EXECUTE_UPON_ACCEPT ? this.paxosState
					.acceptAndUpdateBallot(accept, this.getMyID())
					: this.paxosState.getBallot();
		} catch (Error e) {
			log.severe(this + " : " + e.getMessage());
			Util.suicide(e.getMessage());
		}
		if (ballot == null)
			return null; // can happen only if acceptor is stopped.

		this.garbageCollectAccepted(accept.getMedianCheckpointedSlot());
		if (accept.isRecovery())
			return null; // recovery ACCEPTS do not need any reply

		AcceptReplyPacket acceptReply = new AcceptReplyPacket(this.getMyID(),
				ballot, accept.slot,
				GC_MAJORITY_EXECUTED ? this.paxosState.getSlot() - 1
						: lastCheckpointSlot(this.paxosState.getSlot() - 1,
								accept.getPaxosID()), accept.requestID);

		// no logging if NACking anyway
		AcceptPacket toLog = (accept.ballot.compareTo(ballot) >= 0
		// no logging if already garbage collected or previously accepted
				&& accept.slot - this.paxosState.getGCSlot() > 0 && (prev == null || prev.ballot
				.compareTo(accept.ballot) < 0)) ? accept : null;

		MessagingTask acceptReplyTask = accept.isRecovery() ? new LogMessagingTask(
				toLog) : toLog != null ? new LogMessagingTask(accept.sender,
				acceptReply, toLog) : new MessagingTask(accept.sender,
				acceptReply);
		RequestInstrumenter.sent(acceptReply, this.getMyID(), accept.sender);

		// might release some meta-commits
		PValuePacket reconstructedDecision = this.paxosState
				.reconstructDecision(accept.slot);
		MessagingTask commitTask = reconstructedDecision != null ? this
				.handleCommittedRequest(reconstructedDecision) : null;

		MessagingTask[] mtasks = { acceptReplyTask, commitTask };

		return mtasks;
	}

	/* Batched version of handleAccept, which is meaningful only when request
	 * digests are enabled. Enabling digests is particularly beneficial with one
	 * or small number of active paxos groups that is less than the average size
	 * of a paxos group as it helps balance the coordinator load. With many
	 * paxos groups, digests actually increase the number of messages by n-1 per
	 * paxos round but batching accepts helps reduce that added overhead.
	 * 
	 * With many groups, even with batched accepts, digests are still a net loss
	 * for two reasons. The first is the increased message count. The second is
	 * the added overhead of serializing reconstructed accepts while logging
	 * them. Without digests, serialization for logging purposes comes for free
	 * because we cache the stringified version of the received accept. */
	private static final boolean SHORT_CIRCUIT_LOCAL = Config
			.getGlobalBoolean(PC.SHORT_CIRCUIT_LOCAL);

	private MessagingTask[] handleBatchedAccept(BatchedAccept batchedAccept) {
		assert (BATCHED_ACCEPTS && DIGEST_REQUESTS);
		ArrayList<MessagingTask> mtasks = new ArrayList<MessagingTask>();
		for (Integer slot : batchedAccept.getAcceptSlots()) {
			assert (batchedAccept.getDigest(slot) != null);
			/* Need to put paxosID and version right here as opposed to relying
			 * on the exit procedure because we need that in order to match it
			 * with a request in pendingDigests. */
			AcceptPacket digestedAccept = new AcceptPacket(
					batchedAccept.ballot.coordinatorID, new PValuePacket(
							batchedAccept.ballot, new ProposalPacket(slot,
									(RequestPacket) (new RequestPacket(
											batchedAccept.getRequestID(slot),
											null, false)
											.setDigest(batchedAccept
													.getDigest(slot))
											.putPaxosID(getPaxosID(),
													getVersion())))),
					batchedAccept.getMedianCheckpointedSlot());
			AcceptPacket accept = this.paxosManager.match(digestedAccept);
			if (accept != null) {
				Level level = Level.FINE;
				log.log(level,
						"{0} received matching request for digested accept {1} within batched accept {2}",
						new Object[] { this,
								accept.getSummary(log.isLoggable(level)),
								batchedAccept.getSummary(log.isLoggable(level)) });
				MessagingTask[] mtasksHandleAccept = this.handleAccept(accept);
				if (mtasksHandleAccept != null)
					for (MessagingTask mtask : mtasksHandleAccept)
						if (mtask != null && !mtask.isEmpty())
							mtasks.add(mtask);
			} else {
				assert (!SHORT_CIRCUIT_LOCAL || digestedAccept.sender != getMyID()) : digestedAccept;
				log.log(Level.FINE,
						"{0} received unmatched digested accept {1} within batched accept {2}",
						new Object[] {
								this,
								digestedAccept.getSummary(log
										.isLoggable(Level.FINE)),
								batchedAccept.getSummary(log
										.isLoggable(Level.FINE)) });
			}
		}
		return mtasks.toArray(new MessagingTask[0]);
	}

	/* We don't need to implement this. Accept logs are pruned while
	 * checkpointing anyway, which is enough. Worse, it is probably inefficient
	 * to touch the disk for GC upon every new gcSlot (potentially every accept
	 * and decision). */
	private void garbageCollectAccepted(int gcSlot) {
	}

	/* Phase2b Event: Received a reply to an accept request, i.e. to a request
	 * to accept a proposal from the coordinator.
	 * 
	 * Action: If this reply results in a majority for the corresponding
	 * proposal, commit the request and notify all. If this preempts a proposal
	 * being coordinated because it contains a higher ballot, forward to the
	 * preempting coordinator in the higher ballot reported.
	 * 
	 * Return: The committed proposal if any to be multicast to all replicas, or
	 * the preempted proposal if any to be unicast to the preempting
	 * coordinator. Null if neither. */
	private MessagingTask handleAcceptReply(AcceptReplyPacket acceptReply) {
		this.paxosManager.heardFrom(acceptReply.acceptor); // FD optimization
		RequestInstrumenter.received(acceptReply, acceptReply.acceptor,
				this.getMyID());
		
		Level level=Level.FINER;
		log.log(level, "{0} handling accept reply {1}", new Object[]{this, acceptReply.getSummary(log.isLoggable(level))});

		// handle if undigest request first
		if (acceptReply.isUndigestRequest()) {
			AcceptPacket accept = this.paxosState
					.getAccept(acceptReply.slotNumber);
			assert (accept == null || accept.hasRequestValue());
			log.log(Level.INFO,
					"{0} returning accept {1} for undigest request {2}",
					new Object[] { this,
							accept != null ? accept.getSummary() : accept,
							acceptReply.getSummary() });
			return accept != null ? new MessagingTask(acceptReply.acceptor,
					accept) : null;
		}

		PValuePacket committedPValue = PaxosCoordinator.handleAcceptReply(
				this.coordinator, this.groupMembers, acceptReply);
		if (!PaxosCoordinator.exists(this.coordinator))
			this.coordinator = null; // no-op
		if (committedPValue == null)
			return null;

		MessagingTask multicastDecision = null;
		// separate variables only for code readability
		MessagingTask unicastPreempted = null;
		// could also call handleCommittedRequest below
		if (committedPValue.getType() == PaxosPacket.PaxosPacketType.DECISION) {
			committedPValue.addDebugInfo("d");
			// this.handleCommittedRequest(committedPValue);
			multicastDecision = new MessagingTask(this.groupMembers,
					committedPValue); // inform everyone of the decision
			log.log(Level.FINE,
					"{0} announcing decision {1}",
					new Object[] {
							this,
							committedPValue.getSummary(log
									.isLoggable(Level.FINE)) });
			if (instrument(Integer.MAX_VALUE)) {
				DelayProfiler.updateCount("PAXOS_DECISIONS", 1);
				DelayProfiler.updateCount("CLIENT_COMMITS",
						committedPValue.batchSize() + 1);
			}
		} else if (committedPValue.getType() == PaxosPacket.PaxosPacketType.PREEMPTED) {
			/* Could drop the request, but we forward the preempted proposal as
			 * a no-op to the new coordinator for testing purposes. The new(er)
			 * coordinator information is within acceptReply. Note that our
			 * coordinator status may still be active and it will be so until
			 * all of its requests have been preempted. Note also that our local
			 * acceptor might still think we are the coordinator. The only
			 * evidence of a new coordinator is in acceptReply that must have
			 * reported a higher ballot if we are here, hence the assert.
			 * 
			 * Warning: Can not forward the preempted request as-is to the new
			 * coordinator as this can result in multiple executions of a
			 * request. Although the multiple executions will have different
			 * slot numbers and will not violate paxos safety, this is extremely
			 * undesirable for most applications. 
			 * 
			 * Update: We no longer need to convert preempted requests to a no-op 
			 * before forwarding to the new coordinator because we have support for
			 * handling detecting previously issued accepts for the same
			 * request ID.
			 * */
			assert (committedPValue.ballot.compareTo(acceptReply.ballot) < 0) : (committedPValue
					+ " >= " + acceptReply);
			if (!committedPValue.isNoop()
					// forward only if not already a no-op
					&& (unicastPreempted = new MessagingTask(
							acceptReply.ballot.coordinatorID, committedPValue
//									 .makeNoop()
							.makeNewRequest()
									.setForwarderID(this.getMyID())
									.addDebugInfo("f"))) != null)
				log.log(Level.WARNING,
						"{0} forwarding preempted request to {1}: {2}",
						new Object[] { this, acceptReply.ballot.coordinatorID,
								committedPValue.getSummary() });
			else
				log.log(Level.WARNING,
						"{0} dropping no-op preempted by coordinator {1}: {2}",
						new Object[] { this, acceptReply.ballot.coordinatorID,
				committedPValue.getSummary() });
		}

		if (EXECUTE_UPON_ACCEPT)
			return null;

		return committedPValue.getType() == PaxosPacket.PaxosPacketType.DECISION ? multicastDecision
				: unicastPreempted;
	}

	/* Each accept reply can generate a decision here, so we need to batch the
	 * resulting decisions into a single messaging task. Some of these can be
	 * preempted pvalues as well, so we need to sort them out too (sigh!).
	 * Probably need a cleaner design here. */
	private MessagingTask[] handleBatchedAcceptReply(
			BatchedAcceptReply batchedAR) {
		this.paxosManager.heardFrom(batchedAR.acceptor);
		ArrayList<MessagingTask> preempts = new ArrayList<MessagingTask>();
		ArrayList<MessagingTask> decisions = new ArrayList<MessagingTask>();
		ArrayList<MessagingTask> undigestedAccepts = new ArrayList<MessagingTask>();

		Integer[] acceptedSlots = batchedAR.getAcceptedSlots();
		// DelayProfiler.updateCount("BATCHED_ACCEPT_REPLIES", 1);

		Level level = Level.FINER;
		log.log(level, "{0} handling batched accept reply {1}", new Object[]{this, batchedAR.getSummary(log.isLoggable(level))});

		// sort out decisions from preempted proposals
		for (Integer slot : acceptedSlots) {
			MessagingTask mtask = this.handleAcceptReply(new AcceptReplyPacket(
					batchedAR.acceptor, batchedAR.ballot, slot,
					batchedAR.maxCheckpointedSlot, 0, batchedAR));
			assert (mtask == null || mtask.msgs.length == 1);

			if (mtask != null)
				// preempted noop
				if (((PaxosPacket) mtask.msgs[0]).getType().equals(
						PaxosPacket.PaxosPacketType.REQUEST))
					preempts.add(mtask);
			// good case
				else if (((PaxosPacket) mtask.msgs[0]).getType().equals(
						PaxosPacket.PaxosPacketType.DECISION))
					decisions.add(mtask);
			// undigested accept
				else if (((PaxosPacket) mtask.msgs[0]).getType().equals(
						PaxosPacket.PaxosPacketType.ACCEPT))
					 undigestedAccepts.add(mtask);
				else
					assert (false);
		}

		// batch each of the two into single messaging task
		PaxosPacket[] decisionMsgs = new PaxosPacket[decisions.size()];
		for (int i = 0; i < decisions.size(); i++)
			decisionMsgs[i] = decisions.get(i).msgs[0];

		MessagingTask decisionsMTask = new MessagingTask(this.groupMembers,
				decisionMsgs);

		return MessagingTask.combine(
				MessagingTask.combine(decisionsMTask,
						preempts.toArray(new MessagingTask[0])),
				undigestedAccepts.toArray(new MessagingTask[0]));
	}

	private static final boolean BATCHED_COMMITS = Config
			.getGlobalBoolean(PC.BATCHED_COMMITS);

	/* Phase3 Event: Received notification about a committed proposal.
	 * 
	 * Action: This method is responsible for executing a committed request.
	 * For this, it needs to call a handler implementing the PaxosInterface
	 * interface. */
	private static final boolean LOG_META_DECISIONS = Config
			.getGlobalBoolean(PC.LOG_META_DECISIONS);

	private MessagingTask handleCommittedRequest(PValuePacket committed) {
		assert (committed.getPaxosID() != null);
		//RequestInstrumenter.received(committed, committed.ballot.coordinatorID,this.getMyID());
		if (instrument(!BATCHED_COMMITS)
				&& committed.ballot.coordinatorID != this.getMyID())
			DelayProfiler.updateCount("COMMITS", 1);

		if (!committed.isCoalescable() && !committed.isRecovery()
				&& committed.ballot.coordinatorID != getMyID())
			log.log(Level.INFO, "{0} received syncd decision {1}",
					new Object[] { this, committed.getSummary() });

		PValuePacket correspondingAccept = null, metaDecision=null;
		// log, extract from or add to acceptor, and execute the request at app
		if (!committed.isRecovery()
				&& (committed.hasRequestValue() || LOG_META_DECISIONS))
			AbstractPaxosLogger
					.logDecision(
							this.paxosManager.getPaxosLogger(),
							// only log meta decision if we have the accept
							LOG_META_DECISIONS
									// have corresponding accept
									&& (correspondingAccept = this.paxosState
											.getAccept(committed.slot)) != null
									// and corresponding accept ballot dominates
									&& correspondingAccept.ballot
											.compareTo(committed.ballot) >= 0 ? metaDecision= committed
									.getMetaDecision() :
							/* Could still be a placeholder meta decision as we
							 * may have gotten this decision through a batched
							 * commit without the corresponding accept. We log
							 * meta decisions because they might be useful to
							 * sync other replicas and reduce some logging
							 * overhead for large requests. */
							committed);

		MessagingTask mtask = this.extractExecuteAndCheckpoint(committed);

		if (this.paxosState.getSlot() - committed.slot < 0)
			log.log(Level.FINE,
					"{0} expecting {1}; received out-of-order commit {2} {3}",
					new Object[] { this, this.paxosState.getSlotLog(),
							committed.slot,
							(metaDecision!=null ? metaDecision : committed).getSummary(log.isLoggable(Level.FINE)) });

		return mtask;
	}

	private MessagingTask handleBatchedCommit(BatchedCommit batchedCommit) {
		assert (BATCHED_COMMITS);
		// batched commits can only come directly from the coordinator
		this.paxosManager.heardFrom(batchedCommit.ballot.coordinatorID); // FD
		MessagingTask mtask = null;

		// if (instrument()) DelayProfiler.updateCount("META_COMMITS", 1);

		for (Integer slot : batchedCommit.getCommittedSlots()) {
			// check if we have the corresponding accept
			PValuePacket accept = this.paxosState.getAccept(slot);
			MessagingTask curTask = null;
			if (accept != null && accept.ballot.equals(batchedCommit.ballot)) {
				log.log(Level.FINE,
						"{0} found decision for slot {1} upon receiving {2}",
						new Object[] {
								this,
								slot,
								batchedCommit.getSummary(log
										.isLoggable(Level.FINE)) });
				// keep overwriting mtask with the most recent non-null mtask
				curTask = this
						.handleCommittedRequest(new PValuePacket(accept)
								.makeDecision(batchedCommit
										.getMedianCheckpointedSlot()));
			} else if (BATCHED_COMMITS) {
				log.log(Level.FINE,
						"{0} received slot {1} batched decision {2}, generating placeholder",
						new Object[] {
								this,
								slot,
								batchedCommit.getSummary(log
										.isLoggable(Level.FINE)) });
				// make up a placeholder decision
				curTask = this
						.handleCommittedRequest((PValuePacket) new PValuePacket(
								batchedCommit.ballot, new ProposalPacket(slot,
								// null request value
										new RequestPacket(0, null, false)))
								.makeDecision(
										batchedCommit
												.getMedianCheckpointedSlot())
								.putPaxosID(getPaxosID(), getVersion()));
			}
			if (curTask != null)
				mtask = curTask;
		}
		return mtask;
	}

	private static final boolean DISABLE_SYNC_DECISIONS = Config
			.getGlobalBoolean(PC.DISABLE_SYNC_DECISIONS);
	private static final boolean REVERSE_SYNC = Config
			.getGlobalBoolean(PC.REVERSE_SYNC);

	/* Typically invoked by handleCommittedRequest above. Also invoked at
	 * instance creation time if outOfOrderLimit low to deal with the
	 * progress-wise unsatisfying scenario where a paxos instance gets created
	 * just after other replicas have committed the first few decisions; if so,
	 * the newly starting replica will have no reason to suspect that anything
	 * is missing and may never catch up if no other decisions get committed
	 * (say, because the paxos instance gets stopped before any more decisions).
	 * It is good to prevent such scenarios (even though they don't affect
	 * safety), so we have shouldSync always return true at creation time i.e.,
	 * expected slot is 0 or 1.
	 * 
	 * forceSync is used only in the beginning in the case of missedBirthing. */
	private MessagingTask syncLongDecisionGaps(PValuePacket committed,
			SyncMode syncMode) {

		MessagingTask fixGapsRequest = null;
		if (this.paxosState.canSync(this.paxosManager.getMinResyncDelay())
				&& (this.shouldSync((committed != null ? committed.slot
						: this.paxosState.getMaxCommittedSlot()), this
						.getPaxosManager().getOutOfOrderLimit(), syncMode))) {
			fixGapsRequest = this
					.requestMissingDecisions(committed != null ? committed.ballot.coordinatorID
							: this.paxosState.getBallotCoord(), syncMode);
			if (fixGapsRequest != null) {
				log.log(Level.INFO,
						"{0} sending {1}; maxCommittedSlot = {2}; ",
						new Object[] { this, fixGapsRequest,
								this.paxosState.getMaxCommittedSlot() });
				this.paxosState.justSyncd();
			}
		}
		return fixGapsRequest;
	}

	private MessagingTask syncLongDecisionGaps(PValuePacket committed) {
		return this.syncLongDecisionGaps(committed, SyncMode.DEFAULT_SYNC);
	}

	protected boolean isLongIdle() {
		return this.paxosState.isLongIdle();
	}

	private boolean checkIfTrapped(PaxosPacket incoming, MessagingTask mtask) {
		if (this.isStopped() && mtask != null) {
			log.log(Level.FINE,
					"{0} DROPPING message {1} trapped inside stopped instance",
					new Object[] { this, incoming, mtask });
			return true;
		}
		return false;
	}

	private static enum toLog {
		EEC(false, 100), handlePaxosMessage(false, 100);
		final boolean log;
		final int sampleInt;

		toLog(boolean log, int sampleInt) {
			this.log = log;
			this.sampleInt = sampleInt;
		}

		boolean log() {
			return this.log && instrument(sampleInt);
		}
	};

	private static final int AGREEMENT_LATENCY_SAMPLING = 100;
	private static final int EXECUTION_LATENCY_SAMPLING = 100;

	/* The three actions--(1) extracting the next slot request from the
	 * acceptor, (2) having the app execute the request, and (3) checkpoint if
	 * needed--need to happen atomically. If the app throws an error while
	 * executing the request, we need to retry until successful, otherwise, the
	 * replicated state machine will be stuck. So, essentially, the app has to
	 * support atomicity or the operations have to be idempotent for correctness
	 * of the replicated state machine.
	 * 
	 * This method is protected, not private, because it needs to be called by
	 * the logger after it is done logging the committed request. Having the
	 * logger call this method is only space-efficient design alternative. */
	protected/* synchronized */MessagingTask extractExecuteAndCheckpoint(
			PValuePacket loggedDecision) {
		long methodEntryTime = System.currentTimeMillis();
		int execCount = 0;
		PValuePacket inorderDecision = null;
		synchronized (this) {
			if (this.paxosState.isStopped())
				return null;
			// extract next in-order decision
			while ((inorderDecision = this.paxosState
					.putAndRemoveNextExecutable(loggedDecision)) != null) {
				log.log(inorderDecision.isStopRequest() ? Level.FINE
						: Level.FINE, "{0} received in-order commit {1} {2}",
						new Object[] { this, inorderDecision.slot,
								inorderDecision.getSummary() });
				String pid = this.getPaxosID();

				if (inorderDecision.getEntryReplica() == this.getMyID()
						&& instrument(AGREEMENT_LATENCY_SAMPLING))
					DelayProfiler.updateDelay("agreement",
							inorderDecision.getEntryTime());
				updateRequestBatcher(inorderDecision, loggedDecision == null);

				long t = System.currentTimeMillis();
				/* Execute it until successful, we are *by design* stuck
				 * otherwise. Execution must be atomic with extraction and
				 * possible checkpointing below. */
				if (!EXECUTE_UPON_ACCEPT) // used for testing
					if (execute(this, this.paxosManager, this.getApp(),
							inorderDecision, inorderDecision.isRecovery()))
						// +1 for each batch, not for each constituent
						// requestValue
						execCount++;
					// unclean kill
					else if (this.forceStop())
						break;

				if (instrument(EXECUTION_LATENCY_SAMPLING))
					DelayProfiler.updateDelay(AbstractPaxosLogger.appName
							+ ".execute", t, inorderDecision.batchSize() + 1);

				// getState must be atomic with the execution
				if (shouldCheckpoint(inorderDecision)
						&& !inorderDecision.isRecovery())

					consistentCheckpoint(
							this,
							inorderDecision.isStopRequest(),
							pid,
							this.version,
							this.paxosManager
									.getStringNodesFromIntArray(this.groupMembers),
							inorderDecision.slot, this.paxosState.getBallot(), null,
							this.paxosState
									.getGCSlot());

				/* If stop request, copy epoch final state and kill self. If
				 * copy is not successful, we could get stuck trying to create
				 * future versions for this paxosID. */
				if (inorderDecision.isStopRequest()
						&& this.paxosManager.getPaxosLogger()
								.copyEpochFinalCheckpointState(getPaxosID(),
										getVersion())
						&& (logStop(inorderDecision.getEntryTime())))
					// this.paxosManager.kill(this, true);
					break;
			}
			this.paxosState.assertSlotInvariant();
		}
		/* The kill has been moved out of the synchronized block above as the
		 * synchronized(this) is unnecessary and creates a potential deadlock
		 * with scenarios like pause where paxosManger is first locked and then
		 * this instance's acceptor is locked if in the future we make the
		 * PaxosAcceptor inherit from PaxosInstanceStateMachine. */
		if (inorderDecision != null && inorderDecision.isStopRequest()
				&& this.isStopped())
			this.paxosManager.kill(this, true);

		if (loggedDecision != null && !loggedDecision.isRecovery())
			instrumentDelay(toLog.EEC, methodEntryTime, execCount);
		return loggedDecision != null && !loggedDecision.isRecovery() ? this
				.syncLongDecisionGaps(loggedDecision) : null;
	}

	/* This method synchronizes over paxosManager because otherwise we have no
	 * way of ensuring that a stopped paxos instance does not go ahead and
	 * overwrite a higher version checkpoint. An alternative to implement this
	 * check in putCheckpointState in the logger that anyway does a read before
	 * a write, but it is cleaner to have the following invariant here.
	 * 
	 * Invariant: A paxos instance can not checkpoint if a higher paxos instance
	 * has been (or is being) created. */
	private static final String consistentCheckpoint(PaxosInstanceStateMachine pism,
			boolean isStop, String paxosID, int version, Set<String> members,
			int slot, Ballot ballot, String state, int gcSlot) {
		log.log(Level.FINE, "{0} checkpointing at slot {1}; isStop={2}",
				new Object[] { pism, slot, isStop });
		synchronized (pism.getPaxosManager()) {
			return pism.canCheckpoint() ?
				 AbstractPaxosLogger.checkpoint(pism.getPaxosManager()
						.getPaxosLogger(), isStop, paxosID, version, members,
						slot, ballot, state!=null ? state : pism.getApp().checkpoint(paxosID), gcSlot)
						: null;
		}
	}

	// initial checkpoint or not de-mapped yet
	private boolean canCheckpoint() {
		return this.paxosState.isRecovering()
				|| this.getPaxosManager().isCurrent(getPaxosID(), getVersion());
	}

	private void updateRequestBatcher(PValuePacket inorderDecision,
			boolean handledCP) {
		/* Use entry time only if I am entry replica so that we don't have to
		 * worry about clock synchronization; only if not under recovery; and
		 * only if the decision was received directly as opposed to via
		 * handleCheckpoint.
		 * 
		 * FIXME: should probably exclude all sync decision responses, not just
		 * immediately after handleCheckpoint. */
		if (inorderDecision.getEntryReplica() == getMyID()
				&& !inorderDecision.isRecovery() && !handledCP) {
			assert (inorderDecision.getEntryTime() <= System
					.currentTimeMillis()) : inorderDecision.getEntryTime();
			RequestBatcher.updateSleepDuration(inorderDecision.getEntryTime());
		}
	}

	/**
	 * Helper method used above in EEC as well as by PaxosManager for emulating
	 * unreplicated execution for testing purposes.
	 * 
	 * protected only so that PaxosManager can call this directly to test
	 * emulateUnreplicated mode.
	 */
	protected static final boolean execute(PaxosInstanceStateMachine pism,
			PaxosManager<?> paxosManager, Replicable app,
			RequestPacket decision, boolean recoveryMode) {

		boolean shouldLog = instrument(5 * getCPI(
				paxosManager.getInterCheckpointInterval(),
				decision.getPaxosID()));
		for (RequestPacket requestPacket : decision.getRequestPackets()) {
			boolean executed = false;
			int retries = 0;
			do {
				try {
					/* Note: The conversion below is an important reason for
					 * paxos applications to use RequestPacket as opposed to
					 * propose(String requestValue,...). Otherwise, we have to
					 * unnecessarily encapsulate the string first in a
					 * RequestPacket in PaxosManager and then convert the string
					 * back to InterfaceRequest using the app's getRequest
					 * method. */
					Request request =
					// don't convert to and from string unnecessarily
					!requestPacket.shouldReturnRequestValue() 
					|| requestPacket.requestValue.equals(Request.NO_OP) ? requestPacket
					// ask app to translate string to InterfaceRequest
							: getInterfaceRequest(app,
									requestPacket.getRequestValue());
					Level level = Level.FINE;
					log.log(level,
							"{0} executing (in-order) decision {1}",
							new Object[] {
									pism,
									log.isLoggable(level) ? (request instanceof SummarizableRequest ? ((SummarizableRequest) request)
											.getSummary() : requestPacket
											.getSummary())
											: null });

					if (!((decision instanceof PValuePacket) && ((PValuePacket) decision)
							.isRecovery())
							&& (shouldLog && !(shouldLog = false))) {
						log.log(Level.INFO, "{0} {1}",
								new Object[] { DelayProfiler.getStats(),
										RTTEstimator.print() });
					}

					// TESTPaxosApp tracks noops, so it needs to be fed them
					executed = (requestPacket.requestValue
							.equals(Request.NO_OP) && !(app instanceof TESTPaxosApp))
							|| app.execute(request,
							// do not reply if recovery or not entry replica
									(recoveryMode || (requestPacket
											.getEntryReplica() != paxosManager
											.getMyID())));
					paxosManager.executed(requestPacket,
							request,
							// send response if entry replica and !recovery
							requestPacket.getEntryReplica() == paxosManager
									.getMyID() && !recoveryMode);
					assert (requestPacket.getEntryReplica() > 0) : requestPacket;

					// don't try any more if stopped
					if (pism != null && pism.isStopped())
						return true;
				} catch (Exception | Error e) {
					// must swallow any and all exceptions
					e.printStackTrace();
				}
				if (!executed) {
					String error = paxosManager.getApp(requestPacket
							.getPaxosID())
							+ " failed to execute request, retrying: "
							+ decision.requestValue;
					log.severe(error);
					new RuntimeException(error).printStackTrace();
				}
				/* We have to keep trying to execute until executed to preserve
				 * safety. We have removed the decision from the acceptor and
				 * there is no going back on that by design (as we assume that
				 * invariant at many places). One option here is to kill this
				 * paxos instance after a limited number of retries. The benefit
				 * of doing that is that we can free up this thread. But it is
				 * better to not delete the state on disk just yet as kill()
				 * would do by default. */
				if (++retries > RETRY_LIMIT)
					return false;
			} while (!executed && waitRetry(RETRY_TIMEOUT));
		}
		return true;
	}

	private boolean restore(String state) {
		long t = System.currentTimeMillis();
		boolean restored = this.getApp().restore(getPaxosID(), state);
		DelayProfiler.updateDelay(AbstractPaxosLogger.appName + ".restore", t);
		return restored;
	}

	// Like EEC but invoked upon checkpoint transfer
	private synchronized MessagingTask handleCheckpoint(StatePacket statePacket) {
		if (statePacket.slotNumber >= this.paxosState.getSlot()) {
			// put checkpoint in app (like execute)
			if (!this.restore(statePacket.state))
				return null;
			// update acceptor (like extract)
			this.paxosState.jumpSlot(statePacket.slotNumber + 1);
			// put checkpoint in logger (like checkpoint)
			consistentCheckpoint(this, statePacket.slotNumber == 0,
					this.getPaxosID(), this.version,
					this.paxosManager.getStringNodesFromIntArray(groupMembers),
					statePacket.slotNumber, statePacket.ballot,
					statePacket.state,
					// this.getApp().checkpoint(getPaxosID()),
					this.paxosState.getGCSlot());
			/* A transferred checkpoint is almost definitely not a final
			 * checkpoint as final checkpoints are ephemeral. Even if it is a
			 * final checkpoint, safety is maintained. Just that this replica
			 * may not know that this paxos instance is stopped. */
			log.log(statePacket.slotNumber > 0 ? Level.INFO : Level.FINE,
					"{0} inserted {1} checkpoint through handleCheckpoint; next slot = {2}",
					new Object[] { this,
							statePacket.slotNumber == 0 ? "initial state" : "",
							this.paxosState.getSlotLog() });
		}
		// coz otherwise we can get stuck as assertSlotInvariant() may not hold
		return extractExecuteAndCheckpoint(null);
	}

	/* This method is called by PaxosManager.hibernate that blocks on the
	 * checkpoint operation to finish (unlike regular checkpoints that are
	 * asynchronously handled by a helper thread). But hibernate is currently
	 * not really used as pause suffices. And PaxosManager methods are likely
	 * called by an executor task anyway, so blocking should be harmless. */
	protected synchronized boolean tryForcedCheckpointAndStop() {
		boolean checkpointed = false;
		// Ugly nesting, not sure how else to do this correctly
		synchronized (this.paxosState) {
			synchronized (this.coordinator != null ? this.coordinator
					: this.paxosState) {
				int cpSlot = this.paxosState.getSlot() - 1;
				if (this.paxosState.caughtUp()
						&& PaxosCoordinator.caughtUp(this.coordinator)) {
					String pid = this.getPaxosID();
					consistentCheckpoint(
							this,
							true,
							pid,
							this.getVersion(),
							this.paxosManager
									.getStringNodesFromIntArray(this.groupMembers),
							cpSlot, this.paxosState.getBallot(), null, this.paxosState
									.getGCSlot());
					checkpointed = true;
					log.log(Level.INFO,
							"{0} forcing checkpoint at slot {1}; garbage collected "
									+ "accepts up to slot {2}; max_committed_slot = {3} {4}",
							new Object[] {
									this,
									cpSlot,
									this.paxosState.getGCSlot(),
									this.paxosState.getMaxCommittedSlot(),
									(this.paxosState.getBallotCoordLog() == this
											.getMyID() ? "; maxCommittedFrontier="
											+ PaxosCoordinator
													.getMajorityCommittedSlot(this.coordinator)
											: "") });
					this.forceStop();
				}
			}
		}
		return checkpointed;
	}

	/* Needs to be synchronized so that extractExecuteAndCheckpoint does not
	 * happen concurrently. Likewise handleCheckpoint. */
	protected synchronized boolean forceCheckpoint() {
		String pid = this.getPaxosID();
		int cpSlot = this.paxosState.getSlot() - 1;
		String state = 
		consistentCheckpoint(
				this,
				true,
				pid,
				this.getVersion(),
				this.paxosManager.getStringNodesFromIntArray(this.groupMembers),
				cpSlot, this.paxosState.getBallot(), null, this.paxosState.getGCSlot());
		// need to acquire these without locking
		int gcSlot = this.paxosState.getGCSlot();
		int maxCommittedSlot = this.paxosState.getMaxCommittedSlot();
		String maxCommittedFrontier = (this.paxosState.getBallotCoordLog() == this
				.getMyID() ? "; maxCommittedFrontier="
				+ PaxosCoordinator.getMajorityCommittedSlot(this.coordinator)
				: "");
		log.log(Level.INFO,
				"{0} forcing checkpoint at slot {1}; garbage collected accepts up to slot {2}; "
						+ "max_committed_slot = {3} {4}; state={5}",
				new Object[] { this, cpSlot, gcSlot, maxCommittedSlot,
						maxCommittedFrontier, Util.truncate(state, 128, 128) });
		return true;
	}

	/* A note on locking: The PaxosManager lock is typically the first to get
	 * acquired if it ever appears in a chain of locks with one exception as
	 * noted in the invariants below.
	 * 
	 * Invariants: There must be no lock chain
	 * 
	 * !!! PaxosManager -> PaxosInstanceStateMachine
	 * 
	 * because there is *by design* a lock chain
	 * 
	 * --> PaxosInstanceStateMachine -> PaxosManager
	 * 
	 * when this instance is being stopped.
	 * 
	 * There must be no lock chains as follows (an invariant is easy to adhere
	 * to or rather impossible to violate by design because acceptor and
	 * coordinator are unaware of and have no references to PaxosManager):
	 * 
	 * !!! nothing -> PaxosAcceptor -> PaxosManager
	 * 
	 * !!! nothing -> PaxosCoordinator -> PaxosManager
	 * 
	 * because there are lock chains of the form
	 * 
	 * --> PaxosManager -> PaxosAcceptor or PaxosCoordinator */

	/* Same as tryForcedCheckpointAndStop but without the checkpoint.
	 * 
	 * Why this method is not synchronized: when this paxos instance is
	 * executing a request that takes a long time, this method might
	 * concurrently try to pause it and even succeed (!), say, because the
	 * decision being executed has been extracted and the acceptor looks all
	 * nicely caught up. Is this a problem? The forceStop in this method will
	 * stop the acceptor, but the thread executing EEC will go ahead and
	 * complete the execution and even checkpoint and kill if it is a stop
	 * request. The fact that the acceptor is in a stopped state won't matter
	 * for the current decision being executed. After that, the loop in EEC will
	 * break and return, so no harm done. When this instance gets eventually
	 * unpaused, it would seem exactly like just after having executed that last
	 * decision, so no harm done.
	 * 
	 * Conversely, this method might lock paxosState first and then EEC might
	 * get invoked. If so, the program counter could enter the synchronized EEC
	 * method but will block on paxosState.isStopped until this tryPause method
	 * finishes. If tryPuase is unsuccessful, nothing has changed, so no harm
	 * done. Else if tryPause successfully pauses, isStopped will return true
	 * and EEC will become a noop, so no harm done.
	 * 
	 * Note: If we make this method synchronized, the deactivator thread could
	 * be blocked on this instance for a long time. */
	protected HotRestoreInfo tryPause() {
		// boolean paused = false;
		HotRestoreInfo hri = null;
		synchronized (this.paxosState) {
			// Ugly nesting, not sure how else to do this correctly
			synchronized (this.coordinator != null ? this.coordinator
					: this.paxosState) {
				if (this.paxosState.caughtUp()
						&& PaxosCoordinator.caughtUp(this.coordinator)) {
					hri = new HotRestoreInfo(this.getPaxosID(),
							this.getVersion(), this.groupMembers,
							this.paxosState.getSlot(),
							this.paxosState.getBallot(),
							this.paxosState.getGCSlot(),
							PaxosCoordinator.getBallot(this.coordinator),
							PaxosCoordinator
									.getNextProposalSlot(this.coordinator),
							PaxosCoordinator.getNodeSlots(this.coordinator));
					log.log(Level.FINE, "{0} pausing [{1}]", new Object[] {
							this, hri });
					// if (paused = this.paxosManager.getPaxosLogger().pause(
					// getPaxosID(), hri.toString()))
					this.forceStop();
				} else
					log.log(Level.INFO,
							"{0} not pausing because it is not caught up: {1} {2}",
							new Object[] { this, this.paxosState,
									this.coordinator });
			}
		}
		return hri;
	}

	private boolean shouldCheckpoint(PValuePacket decision) {
		return (decision.slot
				% getCPI(this.paxosManager.getInterCheckpointInterval(),
						decision.getPaxosID()) == 0 || decision.isStopRequest());
	}

	private static final Request getInterfaceRequest(Replicable app, String value) {
		try {
			return app.getRequest(value);
		} catch (RequestParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*************************** End of phase 3 methods ********************************/

	/********************** Start of failure detection and recovery methods *****************/

	/* FIXED: If a majority miss a prepare, the coordinator may never get
	 * elected as follows. The minority of acceptors who did receive the prepare
	 * will assume the prepare's sender as the current coordinator. The rest
	 * might still think the previous coordinator is the current coordinator.
	 * All acceptors could be thinking that their up, so nobody will bother
	 * running for coordinator. To break this impasse, we need to resend the
	 * prepare. This has been now incorporated in the handleMessage that quickly
	 * checks upon every message if we need to "(re)run for coordinator" (for
	 * the same ballot) if we have been waiting for too long (having neither
	 * received a prepare majority nor a preemption) for the ballot to complete. */

	/* Checks whether current ballot coordinator is alive. If not, it checks if
	 * it should try to be the nest coordinator and if so, it becomes the next
	 * coordinator. This method can be called any time safely by any thread. */
	private MessagingTask checkRunForCoordinator() {
		return this.checkRunForCoordinator(false);
	}

	private MessagingTask checkRunForCoordinator(boolean forceRun) {
		Ballot curBallot = this.paxosState.getBallot();
		MessagingTask multicastPrepare = null;
		boolean lastCoordinatorLongDead = this.paxosManager
				.lastCoordinatorLongDead(curBallot.coordinatorID);

		// if(Util.oneIn(20)) log.info(this + " node " + curBallot.coordinatorID
		// + " lastCoordinatorLongDead = " + lastCoordinatorLongDead);

		/* curBallot is my acceptor's ballot; "my acceptor's coordinator" is
		 * that ballot's coordinator.
		 * 
		 * If I am not already a coordinator with a ballot at least as high as
		 * my acceptor's ballot's coordinator
		 * 
		 * AND
		 * 
		 * I didn't run too recently
		 * 
		 * AND
		 * 
		 * (I am my acceptor's coordinator OR (my acceptor's coordinator is dead
		 * AND (I am next in line OR the current coordinator has been dead for a
		 * really long time)))
		 * 
		 * OR forceRun */
		if ((
		/* I am not already a coordinator with a ballot at least as high as my
		 * acceptor's ballot's coordinator && I didn't run too recently */
		!PaxosCoordinator.exists(this.coordinator, curBallot)
				&& !PaxosCoordinator.ranRecently(this.coordinator) &&
		// I am my acceptor's coordinator (can happen during recovery)
		(curBallot.coordinatorID == this.getMyID() ||
		// my acceptor's coordinator is dead
		(!this.paxosManager.isNodeUp(curBallot.coordinatorID) &&
		// I am next in line
		(this.getMyID() == getNextCoordinator(curBallot.ballotNumber + 1,
				this.groupMembers) ||
		// current coordinator has been long dead
				lastCoordinatorLongDead))))

				||
				// haven't run since reboot
		notRunYet()
				// just run
				|| forceRun) {
			/* We normally round-robin across nodes for electing coordinators,
			 * e.g., node 7 will try to become coordinator in ballotnum such
			 * that ballotnum%7==0 if it suspects that the current coordinator
			 * is dead. But it is more robust to check if it has been a long
			 * time since we heard anything from the current coordinator and if
			 * so, try to become a coordinator ourself even though it is not our
			 * turn. Otherwise, weird partitions can result in loss of liveness,
			 * e.g., the next-in-line coordinator thinks the current coordinator
			 * is up but most everyone else thinks the current coordinator is
			 * down. Or the next-in-line coordinator itself could be dead. The
			 * downside of this lastCoordinatorLongDead check is that many nodes
			 * might near simultaneously try to become coordinator with no one
			 * succeeding for a while, but this is unlikely to be a problem if
			 * we rely on the deterministic round-robin rule in the common case
			 * and rely on the lasCoordinatorLongDead with a longer timeout
			 * (much longer than the typical node failure detection timeout). */
			// to avoid invoking synchronized method inside log
			log.log(Level.INFO,
					"{0} running for coordinator as node {1} {2}",
					new Object[] {
							this,
							curBallot.coordinatorID,
							(curBallot.coordinatorID != this.getMyID() ? " seems dead (last pinged "
									+ (this.paxosManager
											.getDeadTime(curBallot.coordinatorID) / 1000)
									+ " secs back)"
									: " has not yet initialized its coordinator") });
			Ballot newBallot = new Ballot(curBallot.ballotNumber + 1,
					this.getMyID());
			if ((this.coordinator = PaxosCoordinator.makeCoordinator(
					this.coordinator, newBallot.ballotNumber,
					newBallot.coordinatorID, this.groupMembers,
					this.paxosState.getSlot(), false)) != null) {
				multicastPrepare = new MessagingTask(this.groupMembers,
						new PreparePacket(newBallot, this.paxosState.getSlot()));
				this.paxosState.setActive2(); // mark as have run at least once
			}
		} else if (PaxosCoordinator.waitingTooLong(this.coordinator)) {
			assert (!PaxosCoordinator.waitingTooLong(this.coordinator)) : this
					+ " " + this.coordinator;
			log.log(Level.WARNING,
					"{0} resending timed out PREPARE {1}; "
							+ "this is only needed under high congestion or reconfigurations or failures",
					new Object[] { this,
							PaxosCoordinator.getBallot(this.coordinator) });
			Ballot newBallot = PaxosCoordinator.remakeCoordinator(
					this.coordinator, groupMembers);
			if (newBallot != null) {
				multicastPrepare = new MessagingTask(this.groupMembers,
						new PreparePacket(newBallot, this.paxosState.getSlot()));
			}
		} else if (!this.paxosManager.isNodeUp(curBallot.coordinatorID)
				&& !PaxosCoordinator.exists(this.coordinator, curBallot)) // not
																			// my
																			// job
			log.log(Level.FINE,
					"{0} thinks current coordinator {1} is {2} dead, the next-in-line is {3}{4}",
					new Object[] {
							this,
							curBallot.coordinatorID,
							(lastCoordinatorLongDead ? "*long*" : ""),
							getNextCoordinator(curBallot.ballotNumber + 1,
									this.groupMembers),
							(PaxosCoordinator.ranRecently(this.coordinator) ? ", and I ran too recently to try again"
									: "") });
		return multicastPrepare;
	}

private boolean notRunYet() {
		return this.paxosState.notRunYet();
}

private String getBallots() {
		return "["
				+ (this.coordinator != null ? "C:("
						+ (this.coordinator != null ? this.coordinator
								.getBallotStr() : "null") + "), " : "")

				+

				"A:("
				+ (this.paxosState != null ? this.paxosState.getBallotSlot()
						: "null") + ")]";
	}

	private String getNodeState() {
		return this.getNodeID() + ":" + this.getPaxosIDVersion() + ":"
				+ this.getBallots();
	}

	/* Computes the next coordinator as the node with the smallest ID that is
	 * still up. We could plug in any deterministic policy here. But this policy
	 * should somehow take into account whether nodes are up or down. Otherwise,
	 * paxos will be stuck if both the current and the next-in-line coordinator
	 * are both dead.
	 * 
	 * It is important to choose the coordinator in a deterministic way when
	 * recovering, e.g., the lowest numbered node. Otherwise different nodes may
	 * have different impressions of who the coordinator is with unreliable
	 * failure detectors, but no one other than the current coordinator may
	 * actually ever run for coordinator. E.g., with three nodes 100, 101, 102,
	 * if 102 thinks 101 is the coordinator, and the other two start by assuming
	 * 100 is the coordinator, then 102's accept replies will keep preempting
	 * 100's accepts but 101 may never run for coordinator as it has no reason
	 * to think there is any problem with 100. */
	private int getNextCoordinator(int ballotnum, int[] members,
			boolean recovery) {
		for (int i = 1; i < members.length; i++)
			assert (members[i - 1] < members[i]);
		assert (!recovery);
		return roundRobinCoordinator(ballotnum);
	}

	private int getNextCoordinator(int ballotnum, int[] members) {
		return this.getNextCoordinator(ballotnum, members, false);
	}

	private int roundRobinCoordinator(int ballotnum) {
		return roundRobinCoordinator(getPaxosID(), this.groupMembers, ballotnum);
	}

	protected static final int roundRobinCoordinator(String paxosID, int[] members,
			int ballotnum) {
		// to load balance coordinatorship across groups
		int randomOffset = paxosID.hashCode();
		return members[(Math.abs(ballotnum + randomOffset)) % members.length];
	}

	/* FIXED: If a majority miss an accept, but any messages are still being
	 * received at all, then the loss will eventually get fixed by a check
	 * similar to checkRunForCoordinator that upon receipt of every message will
	 * poke the local coordinator to recommander the next-in-line accept if the
	 * accept has been waiting for too long (for a majority or preemption). Both
	 * the prepare and accept waiting checks are quick O(1) operations. */

	private static final boolean POKE_COORDINATOR = Config
			.getGlobalBoolean(PC.POKE_COORDINATOR);

	private MessagingTask pokeLocalCoordinator() {
		if (!POKE_COORDINATOR)
			return null;
		AcceptPacket accept = PaxosCoordinator.reissueAcceptIfWaitingTooLong(
				this.coordinator, this.paxosState.getSlot());
		if (accept != null)
			log.log(Level.INFO, "{0} resending timed out ACCEPT {1}",
					new Object[] { this, accept.getSummary() });
		MessagingTask reAccept = (accept != null ? new MessagingTask(
				this.groupMembers, accept) : null);
		return reAccept;
	}

	private boolean logStop(long createTime) {
		if (instrument())
			DelayProfiler.updateDelay("stopcoordination", createTime);
		log.log(Level.INFO, "Paxos instance {0} >>>>STOPPED||||||||||",
				new Object[] { this });
		return true;
	}

	/* Event: Received or locally generated a sync request. Action: Send a sync
	 * reply containing missing committed requests to the requester. If the
	 * requester is myself, multicast to all. */
	private MessagingTask requestMissingDecisions(int coordinatorID, SyncMode syncMode) {
		ArrayList<Integer> missingSlotNumbers = this.paxosState
				.getMissingCommittedSlots(this.paxosManager
						.getMaxSyncDecisionsGap());
		// initially we might want to send an empty sync request
		if (missingSlotNumbers == null)
			return null; // if stopped
		else if (missingSlotNumbers.isEmpty())
			missingSlotNumbers.add(this.paxosState.getSlot());

		int maxDecision = this.paxosState.getMaxCommittedSlot();
		SyncDecisionsPacket srp = new SyncDecisionsPacket(this.getMyID(),
				maxDecision, missingSlotNumbers, this.isMissingTooMuch());

		int requestee = COORD_DONT_LOG_DECISIONS ? randomNonCoordOther(coordinatorID)
				: randomOther();
		int[] requestees = SyncMode.FORCE_SYNC.equals(syncMode) ?
				otherGroupMembers() : new int[1];
		if(!SyncMode.FORCE_SYNC.equals(syncMode))
			requestees[0] = requestee;

		// send sync request to coordinator or random other node or to all
		// other nodes if forceSync
		MessagingTask mtask = requestee != this.getMyID() ? new MessagingTask(
				requestee, srp) : null;

		return mtask;
	}

	private static final boolean COORD_DONT_LOG_DECISIONS = Config
			.getGlobalBoolean(PC.COORD_DONT_LOG_DECISIONS);

	/* We normally sync decisions if the gap between the maximum decision slot
	 * and the expected slot is at least as high as the threshold. But we also
	 * sync in the beginning when the expected slot is 0 (if we disable null
	 * checkpoints) or 1 and there is either a nonzero gap or simply if the
	 * threshold is 1. The initial nonzero gap is an understandable
	 * optimization. But we also sync in the special case when the threshold is
	 * low and this paxos instance has just gotten created (even when there is
	 * no gap) because it is possible that other replicas have committed
	 * decisions that I don't even know have happened. This optimizaiton is not
	 * necessary for safety, but it is useful for liveness especially in the
	 * case when an epoch start (in reconfiguration) is not considered complete
	 * until all replicas have committed the first decision (as in the special
	 * case of reconfigurator node reconfigurations). */
	private static final int INITIAL_SYNC_THRESHOLD = 1;
	private static final int NONTRIVIAL_GAP_FACTOR = 100;

	private boolean shouldSync(int maxDecisionSlot, int threshold,
			SyncMode syncMode) {
		if (DISABLE_SYNC_DECISIONS)
			return false;
		int expectedSlot = this.paxosState.getSlot();
		boolean nontrivialInitialGap = maxDecisionSlot - expectedSlot >= threshold
				/ NONTRIVIAL_GAP_FACTOR;
		boolean smallGapThreshold = threshold <= INITIAL_SYNC_THRESHOLD;

		return
		// typical legitimate sync criterion
		(maxDecisionSlot - expectedSlot >= threshold)
		// sync decisions initially if nonzero gap or small threshold
				|| ((expectedSlot == 0 || expectedSlot == 1) && (nontrivialInitialGap || smallGapThreshold))
				// non-zero gap and syncMode is SYNC_IF_NONZERO_GAP
				|| (nontrivialInitialGap && SyncMode.SYNC_TO_PAUSE
						.equals(syncMode))
				// force sync
				|| SyncMode.FORCE_SYNC.equals(syncMode);

	}

	private boolean shouldSync(int maxDecisionSlot, int threshold) {
		return shouldSync(maxDecisionSlot, threshold, SyncMode.DEFAULT_SYNC);
	}

	private boolean isMissingTooMuch() {
		return this.shouldSync(this.paxosState.getMaxCommittedSlot(),
				this.paxosManager.getMaxSyncDecisionsGap());
	}

	// point here is really to request initial state
	protected MessagingTask requestZerothMissingDecision() {
		ArrayList<Integer> missingSlotNumbers = new ArrayList<Integer>();
		missingSlotNumbers.add(0);

		SyncDecisionsPacket srp = new SyncDecisionsPacket(this.getMyID(), 1,
				missingSlotNumbers, true);

		log.log(Level.INFO, "{0} requesting missing zeroth checkpoint",
				new Object[] { this, });
		// send sync request to coordinator or multicast to others if I am
		// coordinator
		MessagingTask mtask = this.paxosState.getBallotCoord() != this
				.getMyID() ? new MessagingTask(
				this.paxosState.getBallotCoord(), srp) : new MessagingTask(
				otherGroupMembers(), srp);
		return mtask;
	}

	protected int[] otherGroupMembers() {
		return Util.filter(groupMembers, getMyID());
	}

	// random member other than coordinator and self
	private int randomNonCoordOther(int coordinator) {
		if (this.groupMembers.length == 1)
			return this.getMyID(); // no other
		if (this.groupMembers.length == 2 && getMyID() != coordinator)
			return coordinator; // no other option
		// else other exists
		int retval = coordinator;
		// need at least 3 members for this loop to make sense
		while ((retval == getMyID() || retval == coordinator))
			retval = this.groupMembers[(int) (Math.random() * this.groupMembers.length)];
		return retval;
	}

	private int randomOther() {
		if (this.groupMembers.length == 1)
			return this.getMyID(); // no other
		int retval = getMyID();
		// need at least 2 members for this loop to make sense
		while (retval == getMyID() && this.groupMembers.length > 1)
			retval = this.groupMembers[(int) (Math.random() * this.groupMembers.length)];
		return retval;
	}

	/* Event: Received a sync reply packet with a list of missing committed
	 * requests Action: Send back all missing committed requests from the log to
	 * the sender (replier).
	 * 
	 * We could try to send some from acceptor memory instead of the log, but in
	 * general, it is not worth the effort. Furthermore, if the sync gap is too
	 * much, do a checkpoint transfer. */
	private MessagingTask handleSyncDecisionsPacket(
			SyncDecisionsPacket syncReply) throws JSONException {
		int minMissingSlot = syncReply.missingSlotNumbers.get(0);
		log.log(Level.FINE,
				"{0} handling sync decisions request {1} when maxCommittedSlot = {2}",
				new Object[] { this, syncReply.getSummary(),
						this.paxosState.getMaxCommittedSlot() });

		if (this.paxosState.getMaxCommittedSlot() - minMissingSlot < 0)
			return REVERSE_SYNC ? syncLongDecisionGaps(null, SyncMode
					.FORCE_SYNC) : null;
		// I am worse than you

		// get checkpoint if minMissingSlot > last checkpointed slot
		MessagingTask checkpoint = null;
		if (minMissingSlot
				- lastCheckpointSlot(this.paxosState.getSlot(),
						syncReply.getPaxosID()) <= 0) {
			checkpoint = handleCheckpointRequest(syncReply);
			if (checkpoint != null)
				// only get decisions beyond checkpoint
				minMissingSlot = ((StatePacket) (checkpoint.msgs[0])).slotNumber + 1;
		}

		// try to get decisions from memory first
		HashMap<Integer, PValuePacket> missingDecisionsMap = new HashMap<Integer, PValuePacket>();
		for (PValuePacket pvalue : this.paxosState
				.getCommitted(syncReply.missingSlotNumbers))
			missingDecisionsMap.put(pvalue.slot, pvalue.setNoCoalesce());

		// get decisions from database as unlikely to have all of them in memory
		ArrayList<PValuePacket> missingDecisions = this.paxosManager
				.getPaxosLogger()
				.getLoggedDecisions(
						this.getPaxosID(),
						this.getVersion(),
						minMissingSlot,
						/* If maxDecision <= minMissingSlot, sender is probably
						 * doing a creation sync. But we need min < max for the
						 * database query to return nonzero results, so we
						 * adjust up the max if needed. Note that
						 * getMaxCommittedSlot() at this node may not be greater
						 * than minMissingDecision either. For example, the
						 * sender may be all caught up at slot 0 and request a
						 * creation sync for 1 and this node may have committed
						 * up to 1; if so, it should return decision 1. */
						syncReply.maxDecisionSlot > minMissingSlot ? syncReply.maxDecisionSlot
								: Math.max(
										minMissingSlot + 1,
										this.paxosState.getMaxCommittedSlot() + 1));

		// filter non-missing from database decisions
		if (syncReply.maxDecisionSlot > minMissingSlot)
			for (Iterator<PValuePacket> pvalueIterator = missingDecisions
					.iterator(); pvalueIterator.hasNext();) {
				PValuePacket pvalue = pvalueIterator.next();
				if (!syncReply.missingSlotNumbers.contains(pvalue.slot))
					pvalueIterator.remove(); // filter non-missing
				else
					pvalue.setNoCoalesce(); // send as-is, no compacting
				// isRecovery() true only in rollForward
				assert (!pvalue.isRecovery());
			}

		// copy over database decisions not in memory
		for (PValuePacket pvalue : missingDecisions)
			if (!missingDecisionsMap.containsKey(pvalue.slot))
				missingDecisionsMap.put(pvalue.slot, pvalue);

		// replace meta decisions with actual decisions
		getActualDecisions(missingDecisionsMap);

		assert (missingDecisionsMap.isEmpty() || missingDecisionsMap.values()
				.toArray(new PaxosPacket[0]).length > 0) : missingDecisions;
		for (PValuePacket pvalue : missingDecisionsMap.values()) {
			pvalue.setNoCoalesce();
			assert (pvalue.hasRequestValue());
		}
		// the list of missing decisions to be sent
		MessagingTask unicasts = missingDecisionsMap.isEmpty() ? null
				: new MessagingTask(syncReply.nodeID,
						(missingDecisionsMap.values()
								.toArray(new PaxosPacket[0])));

		log.log(Level.INFO,
				"{0} sending {1} missing decision(s) to node {2} in response to {3}",
				new Object[] { this,
						unicasts == null ? 0 : unicasts.msgs.length,
						syncReply.nodeID, syncReply.getSummary() });
		if (checkpoint != null)
			log.log(Level.INFO,
					"{0} sending checkpoint for slot {1} to node {2} in response to {3}",
					new Object[] { this, minMissingSlot - 1, syncReply.nodeID,
							syncReply.getSummary() });

		// combine checkpoint and missing decisions in unicasts
		MessagingTask mtask =
		// both nonempty => combine
		(checkpoint != null && unicasts != null && !checkpoint.isEmpty() && !unicasts
				.isEmpty()) ? mtask = new MessagingTask(syncReply.nodeID,
				MessagingTask
						.toPaxosPacketArray(checkpoint.msgs, unicasts.msgs)) :
		// nonempty checkpoint
				(checkpoint != null && !checkpoint.isEmpty()) ? checkpoint :
				// unicasts (possibly also empty or null)
						unicasts;
		log.log(Level.FINE, "{0} sending mtask: ", new Object[] { this, mtask });
		return mtask;
	}

	/* We reconstruct decisions from logged accepts. This is safe because we
	 * only log a decision with a meta request value when we already have
	 * previously accepted the corresponding accept. */
	private void getActualDecisions(HashMap<Integer, PValuePacket> missing) {
		if (missing.isEmpty())
			return;
		Integer minSlot = null, maxSlot = null;
		// find meta value commits
		for (Integer slot : missing.keySet()) {
			if (missing.get(slot).isMetaValue()) {
				if (minSlot == null)
					minSlot = (maxSlot = slot);
				if (slot - minSlot < 0)
					minSlot = slot;
				if (slot - maxSlot > 0)
					maxSlot = slot;
			}
		}

		if (!(minSlot == null || (minSlot
				- this.paxosState.getMaxAcceptedSlot() > 0))) {

			// get logged accepts for meta commit slots
			Map<Integer, PValuePacket> accepts = this.paxosManager
					.getPaxosLogger().getLoggedAccepts(this.getPaxosID(),
							this.getVersion(), minSlot, maxSlot + 1);

			// reconstruct decision from accept
			for (PValuePacket pvalue : accepts.values())
				if (missing.containsKey(pvalue.slot)
						&& missing.get(pvalue.slot).isMetaValue())
					missing.put(pvalue.slot, pvalue.makeDecision(pvalue
							.getMedianCheckpointedSlot()));
		}

		// remove remaining meta value decisions
		for (Iterator<PValuePacket> pvalueIter = missing.values().iterator(); pvalueIter
				.hasNext();) {
			PValuePacket decision = pvalueIter.next();
			if (decision.isMetaValue()) {
				if (this.paxosState.getSlot() - decision.slot > 0)
					log.log(Level.FINE,
							"{0} has no body for executed meta-decision {1} "
									+ " likely because placeholder decision was "
									+ "logged without corresponding accept)",
							new Object[] { this, decision });
				pvalueIter.remove();
			}
		}
	}

	private int lastCheckpointSlot(int slot, String paxosID) {
		return lastCheckpointSlot(
				slot,
				getCPI(this.paxosManager.getInterCheckpointInterval(),
						this.getPaxosID()));
	}

	private static final int lastCheckpointSlot(int slot, int checkpointInterval) {
		int lcp = slot - slot % checkpointInterval;
		if (lcp < 0 && ((lcp -= checkpointInterval) > 0)) // wraparound-arithmetic
			lcp = lastCheckpointSlot(Integer.MAX_VALUE, checkpointInterval);
		return lcp;
	}

	/* Event: Received a request for a recent checkpoint presumably from a
	 * replica that has recovered after a long down time. Action: Send
	 * checkpoint to requester. */
	private MessagingTask handleCheckpointRequest(SyncDecisionsPacket syncReply) {
		/* The assertion below does not mean that the state we actually get will
		 * be at lastCheckpointSlot() or higher because, even though getSlot()
		 * has gotten updated, the checkpoint to disk may not yet have finished.
		 * We have no way of knowing other than reading the disk. So we first do
		 * a read to check if the checkpointSlot is at least higher than the
		 * minMissingSlot in syncReply. If the state is tiny, this will double
		 * the state fetching overhead as we are doing two database reads. */
		assert (syncReply.missingSlotNumbers.get(0)
				- lastCheckpointSlot(this.paxosState.getSlot(),
						syncReply.getPaxosID()) <= 0);
		int checkpointSlot = this.paxosManager.getPaxosLogger()
				.getCheckpointSlot(getPaxosID());
		StatePacket statePacket = (checkpointSlot >= syncReply.missingSlotNumbers
				.get(0) ? StatePacket.getStatePacket(this.paxosManager
				.getPaxosLogger().getSlotBallotState(this.getPaxosID())) : null);
		if (statePacket != null)
			log.log(Level.INFO,
					"{0} sending checkpoint to node {1}: {2}",
					new Object[] { this, syncReply.nodeID,
							statePacket.getSummary() });
		else {
			String myStatus = (!PaxosCoordinator.exists(this.coordinator) ? "[acceptor]"
					: PaxosCoordinator.isActive(this.coordinator) ? "[coordinator]"
							: "[preactive-coordinator]");
			log.log(Level.INFO, "{0}  has no state (yet) for ", new Object[] {
					this, myStatus, syncReply.getSummary() });
		}

		return statePacket != null ? new MessagingTask(syncReply.nodeID,
				statePacket) : null;
	}

	/*************** End of failure detection and recovery methods ***************/

	/************************ Start of testing and instrumentation methods *****************/
	/* Used only to test paxos instance size. We really need a paxosManager to
	 * do anything real with paxos. */
	private void testingNoRecovery() {
		int initSlot = 0;
		this.coordinator = null;// new PaxosCoordinator();
		if (this.groupMembers[0] == this.getMyID())
			this.coordinator = PaxosCoordinator.makeCoordinator(
					this.coordinator, 0, this.groupMembers[0], groupMembers,
					initSlot, true);
		this.paxosState = new PaxosAcceptor(0, this.groupMembers[0], initSlot,
				null);
	}

	private static final int CREATION_LOG_THRESHOLD = 100000;
	private static int creationCount = 0;

	private static final void incrInstanceCount() {
		creationCount++;
	}

	protected static final void decrInstanceCount() {
		creationCount--;
	}

	// only an approximate count for instrumentation purposes
	private static final int getInstanceCount() {
		return creationCount;
	}

	private static final boolean notManyInstances() {
		return getInstanceCount() < CREATION_LOG_THRESHOLD;
	}

	protected void testingInit(int load) {
		this.coordinator.testingInitCoord(load);
		this.paxosState.testingInitInstance(load);
	}

	protected void garbageCollectDecisions(int slot) {
		this.paxosState.garbageCollectDecisions(slot);
	}

	@Override
	public boolean isPausable() {
		return this.paxosState.isLongIdle();
	}

	protected PaxosInstanceStateMachine markActive() {
		this.paxosState.justActive();
		return this;
	}

	private static final double CPI_NOISE = Config.getGlobalDouble(PC.CPI_NOISE);

	private static final int getCPI(int cpi, String paxosID) {
		return (int) (cpi * (1 - CPI_NOISE) + (Math.abs(paxosID.hashCode()) % cpi)
				* 2 * CPI_NOISE);
	}
}
