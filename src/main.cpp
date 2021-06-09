#include <iostream>
#include <regex>
#include <sstream>
#include <array>
#include <vector>
#include <string>
#include <thread>
#include <stdio.h>
#include <cctype>
#include <chrono>
#include <string.h>
#include <config.h>
#include <unistd.h>
#include "../rapidcsv/rapidcsv.h"

#define PARSING_CHUNK_SIZE 1024 * 1024
#define RTPMSG_LINK_SEARCH "Msg:RTPMSG_LINK"
#define RTPMSG_OPEN_SESSION_SEARCH "Msg:RTPMSG_OPEN_SESSION"
#define RTPMSG_SET_SDP_SEARCH "Msg:RTPMSG_SET_SDP"

using namespace std;

enum RTPControlMessage {
    RTPMSG_OPEN_SESSION,
    RTPMSG_SET_SDP,
    RTPMSG_LINK,
    UNKNOWN
};

RTPControlMessage getControlMessage(const char* rtpControlMessage) {
    if (strcmp("RTPMSG_OPEN_SESSION", rtpControlMessage) == 0) {
        return RTPMSG_OPEN_SESSION;
    }
    if (strcmp("RTPMSG_SET_SDP", rtpControlMessage) == 0) {
        return RTPMSG_SET_SDP;
    }
    if (strcmp("RTPMSG_LINK", rtpControlMessage) == 0) {
        return RTPMSG_LINK;
    }
    return UNKNOWN;
}

struct ALog {
    int cycleStart;
    int cycleWork;
    int cycleSleep;
    int cycleSlept;
    int cycleOverwork;
    int accumOverwork;
};

enum class CallLogType {
    CallSetup,
    CallMaintenance,
    Unknown
};

struct CallLog {
    CallLogType             callLogType;
    int                     startPosition;
    int                     endPosition;
    int                     tid; // ?
    // int                     cycleAvg;
    // int                     AEntryCount;
    vector<ALog>            ALogs;

    CallLog():
        callLogType(CallLogType::Unknown),
        startPosition(-1),
        endPosition(-1),
        tid(-1) {}
};

struct LogFile {
    int             numberOfBytes;
    int             numberOfThreads;
    string          filePath;
    atomic<int>     parsingPosition;
};

struct BLog {
    RTPControlMessage controlMessage;
    string ipAddress;
    int tid;
    BLog(const string& ip,
         const int t,
         RTPControlMessage cm): ipAddress(ip), tid(t), controlMessage(cm){}
};

bool isAlog(string& logline) {
    static regex alogregex(".* (?:RtpSleep\\(\\) cycle start time ).*");
    return regex_match(logline, alogregex);
}

bool isBlog(string& logline) {
    static regex blogregex("(?:(?: Stream Client TCP ).*?TimeUs)");
    return regex_match(logline, blogregex);
}

//(?:(?: Stream Client TCP (\d+\.\d+\.\d+\.\d+\:\d+)(?:(?:.|\s)*?)TID\:(-?\d+)(?:(?:.|\s)*?)Msg\:(RTPMSG_\D+)(?:(?:.|\s))TimeUs:))
bool getALog(ALog& alog, string& logline) {
    static regex ALogRegEx(".*(?:cycle start time\\s+(-?\\d+) us).*(?:cycle work\\s+(-?\\d+) us).*(?:cycle sleep\\s+(-?\\d+) us).*(?:cycle slept\\s+(-?\\d+) us).*(?:cycle overwork\\s+(-?\\d+) us).*(?:accum overwork\\s+(-?\\d+) us)");
    smatch matching;
    if (isAlog(logline)) {
        if (!regex_match(logline, matching, ALogRegEx)) {
            return false;
        }

        auto matchIter = matching.begin();
        alog.cycleStart = stoi(*++matchIter);
        alog.cycleWork = stoi(*++matchIter);
        alog.cycleSleep = stoi(*++matchIter);
        alog.cycleSlept = stoi(*++matchIter);
        alog.cycleOverwork = stoi(*++matchIter);
        alog.accumOverwork = stoi(*++matchIter);

        return true;
    }
    return false;
}

bool getBLogs(vector<BLog>& blogs, const string& log) {
    static regex BLogRegEx("(?:(?:Stream Client TCP (\\d+\\.\\d+\\.\\d+\\.\\d+\\:\\d+) on fd \\d+:\\sRtpLoadRtpMsg\\sTID:[-]?\\d+\\s)?(?:Msg:(\\D+)\\s))");
    smatch matching;
    auto start = log.begin();
    for (;regex_search(start, log.end(), matching, BLogRegEx);) {
        for (auto i = matching.begin(); i != matching.end();) {
            ++i;
        }
        cout << endl;
        cout << endl;
        start = matching[0].second;
    }
    
    return blogs.size() > 0;
}

void printAlog(ALog& alog) {
    cout << alog.cycleStart << " " << alog.cycleSlept << " " << alog.cycleOverwork << endl;
}

void printBlog(BLog& blog) {
    cout << blog.ipAddress << " " << blog.controlMessage << " " << blog.tid << endl;
}

vector<int> getAllPos(const string& chunkBuffer, const regex& reg, const int initialPos) {
    vector<int> positions;
    smatch matching;
    auto start = sregex_iterator(chunkBuffer.begin(), chunkBuffer.end(), reg);
    auto end = sregex_iterator();
    for (auto i = start; i != end; ++i) {
        matching = *i;
        // int pos = matching.position(0) + initialPos;
        positions.push_back(matching.position() + initialPos);
        // fprintf(stdout, "Found %d matches in chunk size %d and initialpos %d at pos %d %s\n", matching.size(), chunkBuffer.size(), initialPos, pos, matching[0].str().c_str());
    }
    return positions;
}

vector<int> getAllLinks(const string& chunkBuffer, const int initialPos) {
    static regex linkRegEx("(?:Msg:RTPMSG_LINK\\s)");
    return getAllPos(chunkBuffer, linkRegEx, initialPos);
}

vector<int> getAllOpen(const string& chunkBuffer, const int initialPos) {
    static regex openRegEx("(?:Msg:RTPMSG_OPEN_SESSION\\s)");
    return getAllPos(chunkBuffer, openRegEx, initialPos);
}

void findAllB(string& chunkBuff, ifstream& fs, vector<int>& links, vector<int>& opens) {
    int initialPos = (int)fs.tellg() - chunkBuff.size();
    // copy until next new line
    for (;chunkBuff[chunkBuff.length() - 1] != ((string::value_type)'\n');) {
        int c = fs.get();
        if (c == fstream::traits_type::eof()) {
            break;
        }
        chunkBuff.append(1, (string::value_type) c);
        //initialPos++;
    }
    auto foundOpens = getAllOpen(chunkBuff, initialPos);// - chunkBuff.size());
    auto foundLinks = getAllLinks(chunkBuff, initialPos);// - chunkBuff.size());

    links.swap(foundLinks);
    opens.swap(foundOpens);

    fs.seekg(initialPos + chunkBuff.size(), fstream::beg);
}

vector<int> mergeLinks(const vector<int>& links) {
    vector<int> mergedLinks;
    for (auto i = links.cbegin(); i != links.cend(); ++i) {
        mergedLinks.push_back(*i++);
    }
    return mergedLinks;
}

vector<int> mergeOpens(const vector<int>& opens) {
    vector<int> mergedOpens;
    for (auto i = opens.cbegin(); i != opens.cend(); ++i) {
        mergedOpens.push_back(*i++);
    }
    return mergedOpens;
}

void getALogsBetween(vector<ALog>& alogs, const int startposition, const int endposition, ifstream& fs) {
    const int initialPosition = fs.tellg();
    string line;
    ALog alog;
    fs.seekg(startposition);
    for (fstream::pos_type position = startposition; position < endposition; position = fs.tellg()) {
        getline(fs, line);
        // fprintf(stdout, "%d \n", position);
        if (getALog(alog, line)) {
            alogs.push_back(alog);
        }
    }

    fs.seekg(initialPosition);
}

// This gets the final position for Step 2
int getEndPositionStep2(const int endposition, ifstream& fs) {
    // include additional 10 A entries after link to be sure work is completed

    const int initialPosition = fs.tellg();
    string line;
    ALog alog;
    fs.seekg(endposition);
    int position = endposition;
    // include additional 10 alogs after link
    for (int alogsAdded = 0; alogsAdded < 10 && !fs.eof();) {
        getline(fs, line);
        if (isAlog(line)) {
            alogsAdded++;
            position = fs.tellg();
        }
        // if (getALog(alog, line)) {
        //     alogs.push_back(alog);
        //     alogsAdded++;
        // }
    }
    fs.seekg(initialPosition);

    return position;
}

// endposition is actually RTPMSG_LINK position
int getStartPositionStep1(const int endposition, ifstream& fs) {
    return getEndPositionStep2(endposition, fs);
}

int getStartPositionStep3(const int endposition, ifstream& fs) {
    // position after RTPMSG_LINK
    return getEndPositionStep2(endposition, fs);
}

double getSumCycleWork(const vector<ALog>& alogs) {
    double sum = 0.0f;

    for (auto alog = alogs.cbegin(); alog != alogs.cend(); alog++) {
        sum += alog->cycleWork;
    }

    return sum;
}

double getAverageCycleWork(const vector<ALog>& alogs) {
    double avg = 0.0f;

    for (auto alog = alogs.cbegin(); alog != alogs.cend(); alog++) {
        avg += alog->cycleWork;
    }

    avg /= alogs.size();

    return avg;
}

void calcRTP(const vector<int>& links, const vector<int>& opens, ifstream& fs, rapidcsv::Document& doc) {
    cout << "calculate RTP..." << endl;
    cout << "There are " << links.size() << " session links." << endl;
    cout << "There are " << opens.size() << " session opens." << endl;
    CallLog callLog;
    // Calculate step 2
    // Call setup
    vector<ALog> alogs;

    vector<CallLog> callSetupLogs;
    // TODO order by log time?
    // Positions should be ordered
    for (auto openposition = opens.cbegin(); openposition != opens.cend(); ++openposition) {
        // move to associated link
        auto linkposition = links.cbegin();
        for (; linkposition != links.end() && (*linkposition) <= (*openposition); ++linkposition);

        // include additional 10 lines in the current call...
        if (*openposition < *linkposition) {
            // fprintf(stdout, "Get alogs between %d %d\n", *openposition, *linkposition);
            auto endposition = getEndPositionStep2(*linkposition, fs);
            getALogsBetween(alogs, *openposition, endposition, fs);
            // Calculate step 2
            callLog.startPosition = *openposition;
            callLog.endPosition = *linkposition;
            callLog.callLogType = CallLogType::CallSetup;

            callLog.ALogs = move(alogs);
            callSetupLogs.push_back(callLog);
            alogs.clear();
        }
    }

    // cout << "There are " << callSetupLogs.size() << " call setup logs" << endl;

    alogs.clear();
    vector<CallLog> callMaintenanceLogs;
    if (*links.cbegin() > *opens.cbegin()) {
        // no link precedings
        // Get from the start of the log until open session
        getALogsBetween(alogs, 0, *opens.cbegin(), fs);

        callLog.startPosition = 0;
        callLog.endPosition = *opens.cbegin();
        callLog.ALogs = move(alogs);
        callLog.callLogType = CallLogType::CallMaintenance;

        callMaintenanceLogs.push_back(callLog);
        alogs.clear();
    }

    alogs.clear();
    for (auto linkposition = links.cbegin(); linkposition != links.cend(); linkposition++) {
        auto openEndPosition = opens.cbegin();
        for (; openEndPosition != opens.cend() && (*linkposition >= *openEndPosition); openEndPosition++);
        const int linkStartPosition = getStartPositionStep3(*linkposition, fs);
        getALogsBetween(alogs, linkStartPosition, *openEndPosition, fs);
        // callLog.linkPosition = *linkposition;
        callLog.startPosition = linkStartPosition;
        callLog.endPosition = *openEndPosition;
        callLog.ALogs = move(alogs);
        callLog.callLogType = CallLogType::CallMaintenance;

        callMaintenanceLogs.push_back(callLog);
        alogs.clear();
    }

    // cout << callMaintenanceLogs.size() << " Call maintenance A entries " << endl;

    // Setup csv

    int nrOfCalls = 0;

    for (auto cm = callMaintenanceLogs.cbegin(); cm != callMaintenanceLogs.cend(); cm++) {

        double callMaintenanceAverage = 0.0f;
        double callSetupAndMaintenanceTotal = 0.0f;
        double callMaitenanceTotal = 0.0f;
        double callSetupTotal = 0.0f;

        auto cs = callSetupLogs.cbegin();
        for (; cs != callSetupLogs.cend() && cs->startPosition != cm->endPosition; cs++);

        if (cs != callSetupLogs.cend()) {
            // fprintf(stdout, "cs->startPosition %d cm->endPosition %d\n", cs->startPosition, cm->endPosition);
            callMaintenanceAverage += getAverageCycleWork(cm->ALogs);
            callSetupAndMaintenanceTotal += getSumCycleWork(cs->ALogs);
            callMaitenanceTotal = callMaintenanceAverage * cs->ALogs.size();
            callSetupTotal = callSetupAndMaintenanceTotal - callMaitenanceTotal;
            nrOfCalls++;

            doc.SetRow<double>(nrOfCalls-1, vector<double>({ (double) nrOfCalls, callMaintenanceAverage, callSetupTotal}) );

            // fprintf(stdout, "#calls %d avgMaintLoop %f TotalSetupTime %f\n", nrOfCalls, callMaintenanceAverage, callSetupTotal);
        }
    }
}

void threadSearch(LogFile& lf,vector<int>& allLinks,vector<int>& allOpens, int chunkSize) {
    try
    {
        vector<int> chunkLinks, chunkOpens;
        std::thread::id this_id = std::this_thread::get_id();
        ifstream fs(lf.filePath, fstream::in);
        string chunkBuffer;
        fs >> noskipws;
        for (int pos=0;pos + chunkSize <= lf.numberOfBytes;) {
            fs.seekg(lf.parsingPosition.fetch_add(chunkSize, std::memory_order::memory_order_acq_rel), ifstream::beg);
            std::this_thread::yield();
            pos = fs.tellg();
            chunkBuffer.clear();
            chunkBuffer.resize(chunkSize);
            fs.read(&chunkBuffer[0], chunkSize);

            if (pos > lf.numberOfBytes) {
                break;
            }

            findAllB(chunkBuffer, fs, chunkLinks, chunkOpens);
            if (chunkLinks.size() > 0) {
                allLinks.insert(allLinks.end(), chunkLinks.begin(), chunkLinks.end());
            }

            if (chunkOpens.size() > 0) {
                allOpens.insert(allOpens.end(), chunkOpens.begin(), chunkOpens.end());
            }

            fprintf(stdout, "\r %f%% done... %d %d", ((double)pos / (double)lf.numberOfBytes) * 100.0f, pos, lf.numberOfBytes);
            fflush(stdout);

        }

        fprintf(stdout, "\n");

        // remaining

        fs.close();
    }
    catch(const exception& e)
    {
        cerr << e.what() << '\n';
    }
}

void process(LogFile& lf, int chunkSize, rapidcsv::Document& doc) {
    try
    {
        ifstream ilf(lf.filePath, fstream::in);
        ilf.seekg(0, ifstream::end);
        lf.numberOfBytes = ilf.tellg();

        cout << "File " << lf.filePath << " has " << lf.numberOfBytes << " bytes." << endl;
        cout.flush();

        vector<vector<int>> threadLinks;
        vector<vector<int>> threadOpens;

        threadLinks.resize(lf.numberOfThreads);
        threadOpens.resize(lf.numberOfThreads);

        vector<thread> threads;
        for (int i = 0; i < lf.numberOfThreads; ++i) {
            threads.emplace_back(thread(threadSearch, ref(lf), ref(threadLinks[i]), ref(threadOpens[i]), chunkSize));
        }

        for_each(threads.begin(), threads.end(), [](thread& t) {
            t.join();
        });

        // Merge all
        vector<int> allLinks;
        vector<int> allOpens;
        for (int i =0; i < threadLinks.size(); ++i) {
            allLinks.insert(allLinks.end(), std::make_move_iterator(threadLinks[i].begin()), std::make_move_iterator(threadLinks[i].end()));
            allOpens.insert(allOpens.end(), std::make_move_iterator(threadOpens[i].begin()), std::make_move_iterator(threadOpens[i].end()));
        }

        std::sort(allLinks.begin(), allLinks.end());
        std::sort(allOpens.begin(), allOpens.end());

        auto mLinks = mergeLinks(allLinks);
        auto mOpens = mergeOpens(allOpens);

        calcRTP(mLinks, mOpens, ilf, doc);
    }
    catch(const exception& e)
    {
        cerr << e.what() << '\n';
    }
}

int main(int argc, char** argv) {

    if (argc < 5) {
        cerr << "Incorrect number of arguments. Specify log file and number of threads and chunk size in bytes and save path\nExample : ./src/ccmslogparser ./ccms_MANUAL_2021-05-06_23-02-11-264_1.log 4 1048576 ./savefile.csv";
        return -1;
    }

    LogFile lf;
    lf.parsingPosition = 0;
    lf.filePath = argv[1];
    lf.numberOfThreads = stoi(argv[2]);
    int chunkSize = stoi(argv[3]);
    string savePath = argv[4];

    rapidcsv::Document doc("", rapidcsv::LabelParams(), rapidcsv::SeparatorParams(','));
    doc.SetColumnName(0, "#Calls");
    doc.SetColumnName(1, "AverageMaintenanceTimePerLoop");
    doc.SetColumnName(2, "TotalSetupTime");

    process(lf, chunkSize, doc);

    doc.Save(savePath);

    cout << endl << "Done." << endl;

    return 0;
}