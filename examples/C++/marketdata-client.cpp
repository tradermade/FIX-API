#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include <quickfix/Application.h>
#include <quickfix/MessageCracker.h>
#include <quickfix/SocketInitiator.h>
#include <quickfix/Session.h>
#include <quickfix/SessionSettings.h>
#include <quickfix/FileStore.h>
#include <quickfix/FileLog.h>

#include <quickfix/fix44/MarketDataRequest.h>
#include <quickfix/fix44/MarketDataRequestReject.h>
#include <quickfix/fix44/MarketDataSnapshotFullRefresh.h>

static std::vector<std::string> splitCSV(const std::string& s) {
  std::vector<std::string> out; std::string cur;
  for (char c : s) { if (c==',') { if(!cur.empty()) out.push_back(cur); cur.clear(); } else cur.push_back(c); }
  if (!cur.empty()) out.push_back(cur);
  return out;
}

class App : public FIX::Application, public FIX::MessageCracker {
 public:
  explicit App(const FIX::SessionSettings& settings) : settings_(settings) {}

  void onCreate(const FIX::SessionID& sid) override { sid_ = sid; }
  void onLogon(const FIX::SessionID& sid) override { std::cout << "Logon: " << sid << "
"; sendSubscribe(); }
  void onLogout(const FIX::SessionID& sid) override { std::cout << "Logout: " << sid << "
"; }

  void toAdmin(FIX::Message& msg, const FIX::SessionID&) noexcept override {
    FIX::MsgType mt; msg.getHeader().getField(mt);
    if (mt == FIX::MsgType_Logon) {
      const auto& d = settings_.get(sid_);
      if (d.has("Username")) msg.setField(FIX::Username(d.getString("Username")));
      if (d.has("Password")) msg.setField(FIX::Password(d.getString("Password")));
    }
  }

  void fromAdmin(const FIX::Message&, const FIX::SessionID&)
    throw(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue, FIX::RejectLogon) override {}

  void toApp(FIX::Message&, const FIX::SessionID&) throw(FIX::DoNotSend) override {}

  void fromApp(const FIX::Message& msg, const FIX::SessionID& sid)
    throw(FIX::FieldNotFound, FIX::IncorrectDataFormat, FIX::IncorrectTagValue, FIX::UnsupportedMessageType) override {
    crack(msg, sid);
  }

  // --- Application messages ---
  void onMessage(const FIX44::MarketDataRequestReject& rej, const FIX::SessionID&) override {
    std::string id = rej.isSetField(262) ? rej.getField(262) : "";
    std::string rs = rej.isSetField(281) ? rej.getField(281) : "";
    std::string tx = rej.isSetField(58)  ? rej.getField(58)  : "";
    std::cout << "MD Reject (35=Y) MDReqID=" << id << " reason(281)=" << rs << " text=" << tx << "
";
  }

  void onMessage(const FIX44::MarketDataSnapshotFullRefresh& snap, const FIX::SessionID&) override {
    std::string sym = snap.getField(FIX::FIELD::Symbol);
    FIX::NoMDEntries n; snap.get(n);
    std::cout << "W: " << sym << " entries=" << n.getValue() << " :: ";
    for (int i=1; i<=n.getValue(); ++i) {
      FIX44::MarketDataSnapshotFullRefresh::NoMDEntries g; snap.getGroup(i, g);
      char t = g.getField(FIX::FIELD::MDEntryType)[0];
      double px = std::stod(g.getField(FIX::FIELD::MDEntryPx));
      std::cout << (t=='0'?"BID":"ASK") << '=' << px << (i<n.getValue()?" | ":"");
    }
    std::cout << '
';
    firstData_.store(true); cv_.notify_all();
  }

  // --- Helpers ---
  void sendSubscribe() {
    const auto& d = settings_.get(sid_);
    FIX44::MarketDataRequest req(FIX::MDReqID("REQ-1"), FIX::SubscriptionRequestType('1'), FIX::MarketDepth(1));
    { FIX44::MarketDataRequest::NoMDEntryTypes e; e.set(FIX::MDEntryType('0')); req.addGroup(e); }
    { FIX44::MarketDataRequest::NoMDEntryTypes e; e.set(FIX::MDEntryType('1')); req.addGroup(e); }
    std::string raw = d.has("Symbols") ? d.getString("Symbols") : "GBPUSD";
    for (auto& s : splitCSV(raw)) { FIX44::MarketDataRequest::NoRelatedSym g; g.set(FIX::Symbol(s)); req.addGroup(g); }
    FIX::Session::sendToTarget(req, sid_);
  }

  void sendUnsubscribeAll() {
    const auto& d = settings_.get(sid_);
    FIX44::MarketDataRequest u(FIX::MDReqID("REQ-1"), FIX::SubscriptionRequestType('2'), FIX::MarketDepth(0));
    std::string raw = d.has("Symbols") ? d.getString("Symbols") : "GBPUSD";
    for (auto& s : splitCSV(raw)) { FIX44::MarketDataRequest::NoRelatedSym g; g.set(FIX::Symbol(s)); u.addGroup(g); }
    FIX::Session::sendToTarget(u, sid_);
  }

  bool waitFirstDataMs(int ms) {
    std::unique_lock<std::mutex> lk(m_);
    return cv_.wait_for(lk, std::chrono::milliseconds(ms), [&]{ return firstData_.load(); });
  }

 private:
  FIX::SessionID sid_;
  const FIX::SessionSettings& settings_;
  std::atomic<bool> firstData_{false};
  std::condition_variable cv_; std::mutex m_;
};

int main(int argc, char** argv) {
  if (argc < 2) { std::cerr << "usage: " << argv[0] << " client.cfg
"; return 2; }
  try {
    FIX::SessionSettings settings(argv[1]);
    App app(settings);
    FIX::FileStoreFactory store(settings);
    FIX::FileLogFactory log(settings);
    FIX::SocketInitiator initiator(app, store, settings, log);

    initiator.start();
    app.waitFirstDataMs(60000); // optional health check

    std::cout << "Running… press Enter to unsubscribe and logout." << std::endl;
    std::cin.get();

    app.sendUnsubscribeAll();
    auto sids = settings.getSessions();
    if (!sids.empty()) if (auto* s = FIX::Session::lookupSession(*sids.begin())) s->logout("Client exit");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    initiator.stop();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "fatal: " << e.what() << "
"; return 1;
  }
}
