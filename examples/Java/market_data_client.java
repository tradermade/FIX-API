// File: MdClient.java
// Compile with: javac -cp quickfixj-core-2.x.jar:slf4j-api.jar MdClient.java
// Run with: java -cp .:quickfixj-core-2.x.jar:slf4j-api.jar:slf4j-simple.jar MdClient client.cfg

import quickfix.*;
import quickfix.field.*;
import quickfix.fix44.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MdClient extends MessageCracker implements Application {
    private final SessionSettings settings;
    private SessionID sid;
    private final CountDownLatch firstData = new CountDownLatch(1);

    public MdClient(SessionSettings settings) {
        this.settings = settings;
    }

    // ---- Lifecycle ----
    @Override public void onCreate(SessionID sessionID) { this.sid = sessionID; }

    @Override public void onLogon(SessionID sessionID) {
        System.out.println("Logon: " + sessionID);
        try { sendSubscribe(); } catch (Exception e) { System.out.println("subscribe error: " + e.getMessage()); }
    }

    @Override public void onLogout(SessionID sessionID) { System.out.println("Logout: " + sessionID); }

    // ---- Admin / App ----
    @Override public void toAdmin(Message message, SessionID sessionID) {
        try {
            String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.LOGON.equals(msgType)) {
                Dictionary d = settings.get(sessionID);
                if (d.isSetField("Username")) message.setField(new Username(d.getString("Username")));
                if (d.isSetField("Password")) message.setField(new Password(d.getString("Password")));
            }
        } catch (Exception ignore) { }
    }

    @Override public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon { }

    @Override public void toApp(Message message, SessionID sessionID) throws DoNotSend { }

    @Override public void fromApp(Message message, SessionID sessionID)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        crack(message, sessionID);
    }

    // ---- Typed handlers ----
    public void onMessage(MarketDataRequestReject rej, SessionID sessionID) throws FieldNotFound {
        String id = rej.isSetField(MDReqID.FIELD) ? rej.getString(MDReqID.FIELD) : "";
        String rs = rej.isSetField(MDReqRejReason.FIELD) ? rej.getString(MDReqRejReason.FIELD) : "";
        String tx = rej.isSetField(Text.FIELD) ? rej.getString(Text.FIELD) : "";
        System.out.println("MD Reject (35=Y) MDReqID=" + id + " reason(281)=" + rs + " text=" + tx);
    }

    public void onMessage(MarketDataSnapshotFullRefresh snap, SessionID sessionID) throws FieldNotFound {
        String sym = snap.getString(Symbol.FIELD);
        int n = snap.isSetField(NoMDEntries.FIELD) ? snap.getInt(NoMDEntries.FIELD) : 0;

        List<String> parts = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            MarketDataSnapshotFullRefresh.NoMDEntries g = new MarketDataSnapshotFullRefresh.NoMDEntries();
            try {
                snap.getGroup(i, g);
                char t = g.getChar(MDEntryType.FIELD);
                String side = (t == MDEntryType.BID) ? "BID" : "ASK";
                double px = Double.parseDouble(g.getString(MDEntryPx.FIELD));
                parts.add(side + "=" + px);
            } catch (FieldNotFound ignore) {
                // skip entries missing required fields
            }
        }
        System.out.println("W: " + sym + " entries=" + n + " :: " + String.join(" | ", parts));
        firstData.countDown();
    }

    // ---- Helpers ----
    private static List<String> splitCSV(String s) {
        List<String> out = new ArrayList<>();
        for (String p : s.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    private List<String> symbolsFromCfg() throws ConfigError {
        Dictionary d = settings.get(sid);
        String raw = d.isSetField("Symbols") ? d.getString("Symbols") : "GBPUSD";
        List<String> syms = splitCSV(raw);
        if (syms.isEmpty()) syms.add("GBPUSD");
        return syms;
    }

    private void sendSubscribe() throws SessionNotFound, ConfigError {
        MarketDataRequest req = new MarketDataRequest(
                new MDReqID("REQ-1"),
                new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_PLUS_UPDATES),
                new MarketDepth(1)
        );
        // 269=0,1
        MarketDataRequest.NoMDEntryTypes e1 = new MarketDataRequest.NoMDEntryTypes();
        e1.setField(new MDEntryType(MDEntryType.BID));
        req.addGroup(e1);
        MarketDataRequest.NoMDEntryTypes e2 = new MarketDataRequest.NoMDEntryTypes();
        e2.setField(new MDEntryType(MDEntryType.OFFER));
        req.addGroup(e2);

        // Symbols 146/55
        for (String s : symbolsFromCfg()) {
            MarketDataRequest.NoRelatedSym g = new MarketDataRequest.NoRelatedSym();
            g.setField(new Symbol(s));
            req.addGroup(g);
        }
        Session.sendToTarget(req, sid);
    }

    private void sendUnsubscribeAll() throws SessionNotFound, ConfigError {
        MarketDataRequest u = new MarketDataRequest(
                new MDReqID("REQ-1"),
                new SubscriptionRequestType(SubscriptionRequestType.DISABLE_PREVIOUS_SNAPSHOT),
                new MarketDepth(0)
        );
        for (String s : symbolsFromCfg()) {
            MarketDataRequest.NoRelatedSym g = new MarketDataRequest.NoRelatedSym();
            g.setField(new Symbol(s));
            u.addGroup(g);
        }
        Session.sendToTarget(u, sid);
    }

    // Wait up to ms for first tick
    public boolean waitFirstDataMs(long ms) {
        try { return firstData.await(ms, java.util.concurrent.TimeUnit.MILLISECONDS); }
        catch (InterruptedException ie) { Thread.currentThread().interrupt(); return false; }
    }

    // ---- Main ----
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("usage: java MdClient client.cfg");
            System.exit(2);
        }
        String cfg = args[0];

        try {
            SessionSettings settings = new SessionSettings(cfg);
            Application app = new MdClient(settings);
            MessageStoreFactory store = new FileStoreFactory(settings);
            LogFactory log = new FileLogFactory(settings);
            Initiator initiator = new SocketInitiator(app, store, settings, log);

            initiator.start();

            // Optional health check
            if (app instanceof MdClient) ((MdClient) app).waitFirstDataMs(60000);

            System.out.println("Running… press Enter to unsubscribe and logout.");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            br.readLine();

            // Unsubscribe and logout
            try { ((MdClient) app).sendUnsubscribeAll(); } catch (Exception e) { System.out.println("unsubscribe error: " + e.getMessage()); }

            // Graceful logout of first session
            @SuppressWarnings("unchecked")
            Iterator<SessionID> it = settings.getSessions().iterator();
            if (it.hasNext()) {
                SessionID sid = it.next();
                Session s = Session.lookupSession(sid);
                if (s != null) s.logout("Client exit");
            }

            Thread.sleep(300);
            initiator.stop();
            System.exit(0);
        } catch (Exception e) {
            System.err.println("fatal: " + e.getMessage());
            System.exit(1);
        }
    }
}
