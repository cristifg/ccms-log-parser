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
#include <cstdlib>
#include <unistd.h>
#include "../rapidcsv/rapidcsv.h"
#ifdef _WIN32
    #include "getopt.h"
#else
    #include <unistd.h>
#endif

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
    long long cycleStart;
    int cycleThrctl;
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
    static regex ALogRegEx(".*(?:cycle start time\\s+(-?\\d+) us).*(?:cycle thrctl\\s+(-?\\d+)).*(?:cycle work\\s+(-?\\d+) us).*(?:cycle sleep\\s+(-?\\d+) us).*(?:cycle slept\\s+(-?\\d+) us).*(?:cycle overwork\\s+(-?\\d+) us).*(?:accum overwork\\s+(-?\\d+) us)");
    smatch matching;
    if (isAlog(logline)) {
        if (!regex_match(logline, matching, ALogRegEx)) {
            return false;
        }

        auto matchIter = matching.begin();
        // fprintf(stdout, "1\n");
        alog.cycleStart = stoll(*++matchIter);
        // fprintf(stdout, "2\n");
        alog.cycleThrctl = stoi(*++matchIter);
        // fprintf(stdout, "3\n");
        alog.cycleWork = stoi(*++matchIter);
        // alog.cycleSleep = stoi(*++matchIter);
        // alog.cycleSlept = stoi(*++matchIter);
        // alog.cycleOverwork = stoi(*++matchIter);
        // alog.accumOverwork = stoi(*++matchIter);

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
        int pos = matching.position() + initialPos;
        if (pos < 0) {
            continue;
        }
        positions.push_back(pos);
        // fprintf(stdout, "Found %d matches in chunk size %d and initialpos %d at pos %d %s\n", matching.size(), chunkBuffer.size(), initialPos, pos, matching[0].str().c_str());
    }
    return positions;
}

vector<int> getAllLinks(const string& chunkBuffer, const int initialPos) {
    // static regex linkRegEx("(?:Msg:RTPMSG_LINK\\s)");
    static regex linkRegEx("(?:ExtAPI: Link\\s)");
    return getAllPos(chunkBuffer, linkRegEx, initialPos);
}

vector<int> getAllOpen(const string& chunkBuffer, const int initialPos) {
    // static regex openRegEx("(?:Msg:RTPMSG_OPEN_SESSION\\s)");
    static regex openRegEx("(?:ExtAPI: OpenSession\\s)");
    return getAllPos(chunkBuffer, openRegEx, initialPos);
}

void findAllEntries(string& chunkBuff, ifstream& fs, vector<int>& links, vector<int>& opens) {
    // fprintf(stdout, "findAll\n");
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
    // fprintf(stdout, "exit findAll\n");
}

vector<int> mergeLinks(const vector<int>& links) {
    vector<int> mergedLinks;
    for (auto i = links.cbegin(); i != links.cend(); ++i) {
        if (i + 1 != links.cend()) {
            mergedLinks.push_back(*(++i));
        }
    }
    return mergedLinks;
}

vector<int> mergeOpens(const vector<int>& opens) {
    vector<int> mergedOpens;
    for (auto i = opens.cbegin(); i != opens.cend(); ++i) {
        if (i + 1 != opens.cend()) {
            mergedOpens.push_back(*(i++));
        }
    }
    return mergedOpens;
}

void getALogsBetween(vector<ALog>& alogs, const int startposition, const int endposition, ifstream& fs, bool verbose = false) {
    const int initialPosition = fs.tellg();
    string line;
    ALog alog;

    fs.seekg(startposition);
    for (fstream::pos_type position = startposition; position < endposition; position = fs.tellg()) {
        getline(fs, line);
        // fprintf(stdout, "%d \n", position);
        if (verbose) {
            fprintf(stdout, "\rGetting ALog entries... %f%% done...", ( ((double)position) / ((double)endposition) * 100.0f ));
        }
        if (getALog(alog, line)) {
            alogs.push_back(alog);
        }
    }

    fs.seekg(initialPosition);

    if (verbose) {
        fprintf(stdout, "\n");
    }
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
        // sum += alog->cycleWork;
        sum += alog->cycleThrctl;
        // fprintf(stdout, "sum %f\n", sum);
    }

    return sum;
}

double getAverageCycleWork(const vector<ALog>& alogs) {
    double avg = 0.0f;
    // int n = 1;
    // for (auto alog = alogs.cbegin(); alog != alogs.cend(); alog++) {
    //     avg = avg* (n - 1)/ n + alog->cycleWork / n; //+= alog->cycleWork;
    //     n++;
    // }

    // int n = 1;
    for (auto alog = alogs.cbegin(); alog != alogs.cend(); alog++) {
        avg += alog->cycleWork;
        // n++;
    }

    avg /= alogs.size();

    return avg;
}

void calcRTP(const vector<int>& links, const vector<int>& opens, ifstream& fs, rapidcsv::Document& doc, const LogFile& lf, bool enableDebug) {
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
    fprintf(stdout, "Preparing call setups logs, please wait...\n");
    fflush(stdout);
    int openCount = 0;
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
            callLog.endPosition = endposition;
            callLog.callLogType = CallLogType::CallSetup;

            callLog.ALogs = move(alogs);
            callSetupLogs.push_back(callLog);
            alogs.clear();

            fprintf(stdout, "\r %f%% done...", (((double)openCount) / (double)opens.size()) * 100.0f);
            fflush(stdout);

            openCount++;
        }
    }
    fprintf(stdout, "\n");

    fprintf(stdout, "Preparing call maintenance logs, please wait...\n");
    fflush(stdout);

    alogs.clear();
    vector<CallLog> callMaintenanceLogs;

    if (*links.cbegin() > *opens.cbegin()) {
        // no link precedings
        // Get from the start of the log until open session

        getALogsBetween(alogs, 0, *opens.cbegin(), fs, true);

        callLog.startPosition = 0;
        callLog.endPosition = *opens.cbegin();
        callLog.ALogs = move(alogs);
        callLog.callLogType = CallLogType::CallMaintenance;

        callMaintenanceLogs.push_back(callLog);
        alogs.clear();
    }

    int linkCount = 0;
    alogs.clear();
    for (auto linkposition = links.cbegin(); linkposition != links.cend(); linkposition++) {
        auto openposition = opens.cbegin();
        for (; openposition != opens.cend() && (*linkposition >= *openposition); openposition++);
        const int linkStartPosition = getStartPositionStep3(*linkposition, fs);
        int openEndPosition = lf.numberOfBytes;

        if (openposition != opens.cend()) {
            openEndPosition = *openposition;
        }

        getALogsBetween(alogs, linkStartPosition, openEndPosition, fs);
        // callLog.linkPosition = *linkposition;
        callLog.startPosition = linkStartPosition;
        callLog.endPosition = openEndPosition;
        callLog.ALogs = move(alogs);
        callLog.callLogType = CallLogType::CallMaintenance;

        callMaintenanceLogs.push_back(callLog);
        alogs.clear();

        fprintf(stdout, "\r %d %f%% done...", linkStartPosition, (((double)linkCount) / (double)links.size()) * 100.0f);
        fflush(stdout);

        linkCount++;
    }

    fprintf(stdout, "\n");

    // Setup csv

    int nrOfCalls = 0;

    // int callSetupsProcessed = 0;

    fprintf(stdout, "Exporting data...\n");

    for (auto cs = callSetupLogs.cbegin(); cs != callSetupLogs.cend(); cs++) {

        double callMaintenanceAverageLoop = 0.0f;
        double callSetupAndMaintenanceTotal = 0.0f;
        // double callMaintenanceTotalTime = 0.0f;
        double callSetupTime = 0.0f;

        auto cm = callMaintenanceLogs.cbegin();
        for (; cm != callMaintenanceLogs.end() && cm->endPosition < cs->startPosition; cm++);

        callMaintenanceAverageLoop += getAverageCycleWork(cm->ALogs);
        // After cycle thrctl was added we can use it to get the total setup time.
        callSetupAndMaintenanceTotal += getSumCycleWork(cs->ALogs);
        if (enableDebug) {
            fprintf(stdout, "callSetupAndMaintenanceTotal : %d\n", (int)callSetupAndMaintenanceTotal);
        }
        // callSetupAndMaintenanceTotal += getSumCycleWork(cm->ALogs);
        // fprintf(stdout, "callMaintenanceAverageLoop : %f\n", callMaintenanceAverageLoop);
        // callMaintenanceTotalTime = ((int)callMaintenanceAverageLoop) * cs->ALogs.size();
        if (enableDebug) {
            fprintf(stdout, "callSetupTime : %d\n", (int)callSetupAndMaintenanceTotal);
        }
        callSetupTime = callSetupAndMaintenanceTotal; //callMaintenanceTotalTime - callSetupAndMaintenanceTotal;

        if (enableDebug) {
            fprintf(stdout, "cs->start: %d cs->end: %d cm->start: %d cm->end: %d\n", cs->startPosition, cs->endPosition, cm->startPosition, cm->endPosition);
        }
        doc.SetRow<int>(nrOfCalls, vector<int>({ nrOfCalls, (int)callMaintenanceAverageLoop, (int)cs->ALogs.size() , (int)callSetupTime}) );
        // fprintf(stdout, "Total maintenance time %f\n", callSetupAndMaintenanceTotal);
        // fprintf(stdout, "Average between %d %d : %f\n", cm->startPosition, cm->endPosition, callMaintenanceAverageLoop);
        if (enableDebug) {
            fprintf(stdout, "#calls %d avgMaintLoop %dusec NumSetupLoops %d TotalSetupTime %dusec\n", nrOfCalls, (int)callMaintenanceAverageLoop, (int) cs->ALogs.size(), (int)callSetupTime);
            fprintf(stdout, "\n");
        }
        // fprintf(stdout, "#calls %d avg %f \n", nrOfCalls, callMaintenanceAverageLoop);

        // fprintf(stdout, "\r %f%% done...", (((double)callSetupsProcessed) / (double)callSetupLogs.size()) * 100.0f);
        // fflush(stdout);
        // callSetupsProcessed++;

        nrOfCalls++;
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

            findAllEntries(chunkBuffer, fs, chunkLinks, chunkOpens);
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

void process(LogFile& lf, int chunkSize, rapidcsv::Document& doc, bool enableDebug) {
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

        if (enableDebug) {
            fprintf(stdout, "Merging results...\n");
        }

        // Merge all
        vector<int> allLinks;
        vector<int> allOpens;
        for (int i =0; i < threadLinks.size(); ++i) {
            allLinks.insert(allLinks.end(), std::make_move_iterator(threadLinks[i].begin()), std::make_move_iterator(threadLinks[i].end()));
            allOpens.insert(allOpens.end(), std::make_move_iterator(threadOpens[i].begin()), std::make_move_iterator(threadOpens[i].end()));
        }

        // for (auto l : allLinks) {
        //     fprintf(stdout, "link %d\n", l);
        // }

        std::sort(allLinks.begin(), allLinks.end());

        if (enableDebug) {
            fprintf(stdout, "%d links\n", allLinks.size());
        }

        // fprintf(stdout, "\n");

        // for (auto l : allLinks) {
        //     fprintf(stdout, "link %d\n", l);
        // }

        // for (auto l : allOpens) {
        //     fprintf(stdout, "open %d\n", l);
        // }

        std::sort(allOpens.begin(), allOpens.end());
        // fprintf(stdout, "\n");
        // for (auto l : allOpens) {
        //     fprintf(stdout, "opens %d\n", l);
        // }

        // We don't need to merge if logs use ExtAPI
        auto mLinks = move(allLinks); //mergeLinks(allLinks);

        if (enableDebug) {
            fprintf(stdout, "%d opens\n", allOpens.size());
        }

        // for (auto ml : mLinks) {
        //     fprintf(stdout, "ml %d\n", ml);
        // }

        auto mOpens = /*move(allOpens); //*/ mergeOpens(allOpens);
        // for (auto mo : mOpens) {
        //     fprintf(stdout, "mo %d\n", mo);
        // }
        calcRTP(mLinks, mOpens, ilf, doc, lf, enableDebug);
    }
    catch(const exception& e)
    {
        cerr << e.what() << '\n';
    }
}

int main(int argc, char** argv) {

    int c;
    int chunkSize;
    string savePath;
    bool enableDebug = false;
    LogFile lf;
    while(1) {
        c = getopt(argc, argv, "t:c:o:dh");

        if (c == -1)
            break;

        switch(c) {
            case 't': {
                lf.numberOfThreads = stoi(optarg);
                break;
            }
            case 'c': {
                chunkSize = stoi(optarg);
                break;
            }
            case 'o': {
                savePath = optarg;
                break;
            }
            case 'd': {
                enableDebug = true;
                break;
            }
            case 'h': {
                fprintf(stdout, "--- CCMS LOG PARSER ---\nHelps parse and analyze CCMS log files.\n\nUsage: ccmslogparser [-t threads] [-c chunksize] [-o outputpath] [-d] inputpath\n\nt : number of threads to use\nc : chunk size for each thread, log file is split into chunk\no : output file path - csv file\nd : if this flag is specified additional info will be printed to standard output\nThe last argument is the input log file path.\n");
                return 0;
            }
        }
    }

    if (optind != argc-1) {
        cerr << "Incorrect number of arguments. Specify log file and number of threads and chunk size in bytes and save path\nExample : ./src/ccmslogparser ./ccms_MANUAL_2021-05-06_23-02-11-264_1.log 4 1048576 ./savefile.csv";
        return 1;
    }

    cout << "Processing log file: " << argv[optind] << endl;
    lf.filePath = string(argv[optind]);
    // if (argc < 5) {
    //     cerr << "Incorrect number of arguments. Specify log file and number of threads and chunk size in bytes and save path\nExample : ./src/ccmslogparser ./ccms_MANUAL_2021-05-06_23-02-11-264_1.log 4 1048576 ./savefile.csv";
    //     return -1;
    // }

    try {
        ifstream f(lf.filePath);
        if (!f.good()) {
            fprintf(stderr, "Failed to open file %s\n", lf.filePath.c_str());
            return -1;
        }
        f.close();
    } catch(const std::exception& ex) {
        fprintf(stderr, "Failed to open file %s\n %s", lf.filePath.c_str(), ex.what());
        return -1;
    }

    lf.parsingPosition = 0;
    // lf.filePath = argv[1];
    // lf.numberOfThreads = stoi(argv[2]);
    // int chunkSize = stoi(argv[3]);
    // string savePath = argv[4];

    rapidcsv::Document doc("", rapidcsv::LabelParams(), rapidcsv::SeparatorParams(','));
    doc.SetColumnName(0, "#Calls");
    doc.SetColumnName(1, "AverageMaintenanceTimePerLoop");
    doc.SetColumnName(2, "NumSetupLoops");
    doc.SetColumnName(3, "TotalSetupTime");

    process(lf, chunkSize, doc, enableDebug);

    doc.Save(savePath);

    cout << endl << "Done." << endl;

    return 0;
}