import quickfix as fix
import quickfix44 as fix44
import sys
import threading
import time

class MarketDataApp(fix.Application, fix.MessageCracker):
    """
    FIX Application class implementing the client logic for subscribing 
    to and handling market data snapshots (FIX 4.4 MarketDataSnapshotFullRefresh).
    """

    def __init__(self, settings):
        self.settings = settings
        self.session_id = None
        # Use threading.Event to signal when the first market data message is received.
        self.first_data_event = threading.Event()
        print("QuickFIX Python Market Data Client initialized.")

    # --- Standard FIX Application Callbacks ---
    def onCreate(self, sessionID):
        """Called when a new session is created."""
        pass

    def onLogon(self, sessionID):
        """Called when a session successfully logs on."""
        self.session_id = sessionID
        print(f"Logon: {sessionID}")
        self.sendSubscribe()

    def onLogout(self, sessionID):
        """Called when a session logs out."""
        print(f"Logout: {sessionID}")

    def toAdmin(self, message, sessionID):
        """
        Allows modification of admin messages (like Logon).
        Adds Username/Password fields if present in the configuration.
        """
        msg_type = fix.MsgType()
        message.getHeader().getField(msg_type)
        if msg_type.getValue() == fix.MsgType_Logon:
            try:
                # Retrieve session-specific settings
                session_dict = self.settings.get(sessionID)

                if session_dict.has('Username'):
                    message.setField(fix.Username(session_dict.getString('Username')))
                if session_dict.has('Password'):
                    message.setField(fix.Password(session_dict.getString('Password')))
            except fix.ConfigError as e:
                # Handle case where sessionID is not found in settings
                print(f"Error accessing settings for {sessionID}: {e}")
            except fix.FieldNotFound:
                # Field not found is expected if Username/Password isn't set
                pass

    def fromAdmin(self, message, sessionID):
        """Called when an admin message is received from the counterparty."""
        pass

    def toApp(self, message, sessionID):
        """Called before an application message is sent to the counterparty."""
        pass

    def fromApp(self, message, sessionID):
        """
        Entry point for application-level messages. Cracks the message 
        to call the specific onMessage handler.
        """
        self.crack(message, sessionID)

    # --- FIX MessageCracker Handlers ---

    def onMessage(self, message, sessionID):
        """Handles FIX44::MarketDataSnapshotFullRefresh (35=W)"""
        try:
            # Cast message to the specific type for easy field access
            snap = fix44.MarketDataSnapshotFullRefresh(message)

            sym = snap.get(fix.Symbol()).getValue()
            n = snap.get(fix.NoMDEntries()).getValue()

            output = [f"W: {sym} entries={n} :: "]

            # Iterate through the repeating group (NoMDEntries)
            for i in range(1, n + 1):
                group = fix44.MarketDataSnapshotFullRefresh.NoMDEntries()
                snap.getGroup(i, group)

                t = group.get(fix.MDEntryType()).getValue()
                px = group.get(fix.MDEntryPx()).getValue()

                type_str = "BID" if t == '0' else "ASK"
                output.append(f"{type_str}={px}")

            print(' | '.join(output))

            # Signal that market data has been received
            self.first_data_event.set()

        except fix.FieldNotFound as e:
            print(f"Error processing MD Snapshot (Field Not Found): {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing MD Snapshot: {e}")

    def onMessage(self, message, sessionID):
        """Handles FIX44::MarketDataRequestReject (35=Y)"""
        rej = fix44.MarketDataRequestReject(message)

        md_req_id = rej.isSetField(fix.MDReqID()) and rej.get(fix.MDReqID()).getValue() or ""
        rej_reason = rej.isSetField(fix.MDReqReqID()) and rej.get(fix.MDReqReqID()).getValue() or ""
        text = rej.isSetField(fix.Text()) and rej.get(fix.Text()).getValue() or ""

        print(f"MD Reject (35=Y) MDReqID={md_req_id} reason(281)={rej_reason} text={text}")

    # --- Helpers ---

    def _get_symbols_from_settings(self):
        """Retrieves and splits the Symbols string from settings."""
        if self.session_id is None:
            return []

        try:
            session_dict = self.settings.get(self.session_id)
            symbols_raw = session_dict.has("Symbols") and session_dict.getString("Symbols") or "GBPUSD"
            return [s.strip() for s in symbols_raw.split(',') if s.strip()]
        except (fix.ConfigError, fix.RuntimeError) as e:
            print(f"Error retrieving Symbols from settings: {e}")
            return ["GBPUSD"]

    def sendSubscribe(self):
        """Sends a FIX 4.4 Market Data Request (35=V) to subscribe to level 1 data."""
        if not self.session_id: return

        # 1. Create the request message (MDReqID, SubReqType='1', MarketDepth=1)
        req = fix44.MarketDataRequest(
            fix.MDReqID("REQ-1"),
            fix.SubscriptionRequestType('1'), # '1' = Snapshot + Updates
            fix.MarketDepth(1) # Level 1 data (top of book)
        )

        # 2. Add MDEntryType Groups (Bid '0' and Offer '1')
        entries = fix44.MarketDataRequest.NoMDEntryTypes()
        entries.set(fix.MDEntryType('0')) # Bid
        req.addGroup(entries)
        entries.set(fix.MDEntryType('1')) # Offer
        req.addGroup(entries)

        # 3. Add Symbol Groups (NoRelatedSym)
        for symbol in self._get_symbols_from_settings():
            sym_group = fix44.MarketDataRequest.NoRelatedSym()
            sym_group.set(fix.Symbol(symbol))
            req.addGroup(sym_group)

        try:
            fix.Session.sendToTarget(req, self.session_id)
            print(f"Sent MD Subscription for: {', '.join(self._get_symbols_from_settings())}")
        except fix.SessionNotFound as e:
            print(f"Error: Session not found when attempting to send subscription: {e}")

    def sendUnsubscribeAll(self):
        """Sends a FIX 4.4 Market Data Request (35=V) to unsubscribe (SubReqType='2')."""
        if not self.session_id: return

        # 1. Create the request message (MDReqID, SubReqType='2', MarketDepth=0)
        unsub = fix44.MarketDataRequest(
            fix.MDReqID("REQ-1"),
            fix.SubscriptionRequestType('2'), # '2' = Unsubscribe
            fix.MarketDepth(0)
        )

        # 2. Add Symbol Groups (NoRelatedSym)
        for symbol in self._get_symbols_from_settings():
            sym_group = fix44.MarketDataRequest.NoRelatedSym()
            sym_group.set(fix.Symbol(symbol))
            unsub.addGroup(sym_group)

        try:
            fix.Session.sendToTarget(unsub, self.session_id)
            print(f"Sent MD Unsubscribe for: {', '.join(self._get_symbols_from_settings())}")
        except fix.SessionNotFound as e:
            print(f"Error: Session not found when attempting to send unsubscribe: {e}")

    def wait_for_first_data(self, timeout_ms):
        """
        Blocking call that waits for the first market data message to be received.
        Returns True if data was received, False otherwise.
        """
        timeout_seconds = timeout_ms / 1000.0
        print(f"Waiting up to {timeout_seconds} seconds for initial market data...")
        return self.first_data_event.wait(timeout_seconds)

def main():
    if len(sys.argv) < 2:
        print("usage: python market_data_client.py client.cfg")
        sys.exit(2)

    try:
        # 1. Load configuration and setup application components
        settings = fix.SessionSettings(sys.argv[1])
        application = MarketDataApp(settings)
        store_factory = fix.FileStoreFactory(settings)
        log_factory = fix.FileLogFactory(settings)

        # 2. Initialize the SocketInitiator
        # Note: Threading is managed internally by the QuickFIX library
        initiator = fix.SocketInitiator(application, store_factory, settings, log_factory)

        # 3. Start the initiator (connects and attempts logon)
        initiator.start()

        # 4. Optional Health Check: Wait for initial data
        if not application.wait_for_first_data(60000):
            print("Warning: Timed out waiting for initial market data.")

        print("Running... press Enter to unsubscribe and logout.")
        sys.stdin.readline()

        # 5. Cleanup: Unsubscribe and Logout
        application.sendUnsubscribeAll()
        time.sleep(0.3) # Give time for unsubscribe message to be processed

        # Logout the session
        sids = settings.getSessions()
        if sids:
            session = fix.Session.lookupSession(sids[0])
            if session:
                session.logout("Client exit")
                print("Sent Logout message.")

        initiator.stop()

    except fix.ConfigError as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
