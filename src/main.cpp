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
    auto start = chunkBuffer.cbegin();
    for (;regex_search(start, chunkBuffer.cend(), matching, reg);) {
        for (int i = 0; i < matching.size(); ++i) {
            positions.push_back(matching.position(i + 1) + initialPos);
        }
        start = matching[0].second;
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
    int initialPos = fs.tellg();
    // copy until next new line
    for (;chunkBuff[chunkBuff.length() - 1] != ((string::value_type)'\n');) {
        int c = fs.get();
        if (c == fstream::traits_type::eof()) {
            break;
        }
        chunkBuff.append(1, (string::value_type) c);
    }

    auto foundLinks = getAllLinks(chunkBuff, initialPos);
    auto foundOpens = getAllOpen(chunkBuff, initialPos);

    links.swap(foundLinks);
    opens.swap(foundOpens);

    fs.seekg(initialPos, fstream::beg);
}

void calcStep1RTP(const vector<int>& links, const vector<int>& opens) {

}

void calcRTP(const vector<int>& links, const vector<int>& opens) {

    // cout << "There are " << links.size() << " links" << endl;
    // cout << "There are " << opens.size() << " opens" << endl;


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
                allLinks.insert(allLinks.begin(), chunkLinks.begin(), chunkLinks.end());
            }

            if (chunkOpens.size() > 0) {
                allOpens.insert(allOpens.begin(), chunkOpens.begin(), chunkOpens.end());
            }

            fprintf(stdout, "\r %f%% done... %d %d", ((double)pos / (double)lf.numberOfBytes) * 100.0f, pos, lf.numberOfBytes);
            fflush(stdout);

        }

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

        calcRTP(allLinks, allOpens);
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