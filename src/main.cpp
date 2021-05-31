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

struct CallLog {
    int             position;
    int             tid; // ?
    int             cycleAvg;
    CallLog():
        position(-1),
        tid(-1),
        cycleAvg(0) {}
};

struct LogFile {
    int             numberOfBytes;
    int             numberOfThreads;
    string          filePath;
    atomic<int>     parsingPosition;
};

struct ALog {
    int cycleStart;
    int cycleWork;
    int cycleSleep;
    int cycleSlept;
    int cycleOverwork;
    int accumOverwork;
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
            // cout << (++i)->str() << endl;
            // cout << (++i)->str() << endl;
            // cout << (++i)->str() << endl;
            // blogs.emplace_back((++i)->str(),
            //                    stoi((++i)->str()),
            //                    getControlMessage((++i)->str().c_str()));
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
    // auto start = chunkBuffer.cbegin();
    // for (;regex_search(start, chunkBuffer.cend(), matching, reg);) {
    //     positions.push_back(matching.position() + initialPos);
    //     // for (int i = 0; i < matching.size(); ++i) {
    //     //     // if (initialPos == 0) {
    //             int pos = matching.position(0) + initialPos;
    //     //         // fprintf(stdout, "MATCH : %s pos : %d\n", matching[i].str().c_str(), pos);
    //     //     // }
    //         fprintf(stdout, "Found %d matches in chunk size %d and initialpos %d at pos %d %s\n", matching.size(), chunkBuffer.size(), initialPos, pos, matching[0].str().c_str());
    //     //     positions.push_back(matching.position(i + 1) + initialPos);
    //     //     // fprintf(stdout, "pos : %d \n", pos);
    //     // }
    //     // start = matching[0].second;
    //     start = matching[0].second;
    // }

    // fprintf(stdout, "\n");
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

        if (getALog(alog, line)) {
            alogs.push_back(alog);
        }
    }

    fs.seekg(initialPosition);
}

// endposition is actually RTPMSG_LINK position
int getStartPositionStep1(const int endposition, ifstream& fs) {
    return getEndPositionStep2(endposition, fs);
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
    for (int alogsAdded = 0; alogsAdded < 10 || !fs.eof();) {
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

void calcRTP(const vector<int>& links, const vector<int>& opens, ifstream& fs) {
    cout << "calculate RTP..." << endl;
    cout << "There are " << links.size() << " session links." << endl;
    cout << "There are " << opens.size() << " session opens." << endl;
    vector<CallLog> callLogs;
    CallLog callLog;
    // Calculate step 2
    vector<int> beforeOpenLinks;
    vector<ALog> alogs;
    callLog.cycleAvg = 0;

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
            callLog.position = *openposition;
            for (auto alog = alogs.cbegin(); alog != alogs.cend(); ++alog) {
                callLog.cycleAvg += alog->cycleWork;
            }
            callLog.cycleAvg = callLog.cycleAvg / alogs.size();
            callLogs.push_back(callLog);
            alogs.clear();
        }
    }

    cout << "There are " << callLogs.size() << " calls" << endl;

    // Step 1
    callLog.cycleAvg = 0;
    if (*links.cbegin() < *opens.cbegin()) {
        // with link precedings
        cout << "There are link precedings..." << endl;
        vector<ALog> alogsPreceding;
        // for (auto link1 = beforeOpenLinks.cbegin(); link1 != beforeOpenLinks.cend() && (*link1 < *opens.cbegin()) ; ++link1) {
        //     cout << "Get alog between " << *link1 << " " << *opens.cbegin() << endl;
        int startPosition = getStartPositionStep1(*links.cbegin(), fs);
        getALogsBetween(alogsPreceding, startPosition, *opens.cbegin(), fs);
        // }

        // Get average without 10 additional
        // TODO
        for (auto alog = alogsPreceding.cbegin(); alog != alogsPreceding.cend(); ++alog) {

            // cout << "Add " << alog->cycleWork << endl;
            callLog.cycleAvg += alog->cycleWork;
        }

        callLog.cycleAvg = callLog.cycleAvg / alogsPreceding.size();

        cout << "Average cycle work start : " << callLog.cycleAvg << endl;
    } else {
        // no link precedings
        // Get from the start of the log until open session
        vector<ALog> alogsFromStart;
        getALogsBetween(alogsFromStart, 0, *opens.cbegin(), fs);
        for (auto alog = alogsFromStart.cbegin(); alog != alogsFromStart.cend(); ++alog) {
            // cout << "Add " << alog->cycleWork << endl;
            callLog.cycleAvg += alog->cycleWork;
        }

        cout << "Average cycle work start before RTPMSG_OPEN_SESSION: " << callLog.cycleAvg << endl;
    }

    // cout << "There are : " << alogs.size() << " A entries." << endl;

    // Calculate step 3
    // Get control message time...
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

        // Check all links
        // calcRTP(allLinks, allOpens);

        fs.close();
    }
    catch(const exception& e)
    {
        cerr << e.what() << '\n';
    }
}

void preprocess(LogFile& lf, int chunkSize) {
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

        calcRTP(mLinks, mOpens, ilf);
    }
    catch(const exception& e)
    {
        cerr << e.what() << '\n';
    }
}

int main(int argc, char** argv) {

    if (argc < 4) {
        cerr << "Incorrect number of arguments. Specify log file and number of threads and chunk size in bytes.";
        return -1;
    }

    LogFile lf;
    lf.parsingPosition = 0;
    lf.filePath = argv[1];
    lf.numberOfThreads = stoi(argv[2]);
    int chunkSize = stoi(argv[3]);

    preprocess(lf, chunkSize);

    cout << endl << "Parsing done." << endl;

    return 0;
    // Write to csv
    return 0;
}