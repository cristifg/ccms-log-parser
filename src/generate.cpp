#include <iostream>
#include <fstream>
#include <random>

using namespace std;

int getCycleStartTime(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(334229311 - 1000, 334229311);
    return distr(gen);
}

int getCycleWork(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(248 - 200, 248);
    return distr(gen);
}

int getCycleSleep(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(0, 5000);
    return distr(gen);
}

int getCycleSleept(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(0, 10);
    return distr(gen);
}

int getCycleOverwork(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(-5000, 5000);
    return distr(gen);
}

int getAccumOverwork(std::mt19937 &gen) {
    static std::uniform_int_distribution<> distr(0, 300);
    return distr(gen);
}

int main(int argc, char** argv) {

    std::random_device rd;
    std::mt19937 generator(rd());
    std::bernoulli_distribution d(0.25);

    try
    {
        ofstream outputFile("./ccmslog");
        for (int i = 0; i < 100; ++i) {
            outputFile << "23:04:11.847447 pri=1:RTP0_THREAD:00000000 [rtpthreads/src/common.c:329] RtpSleep() cycle start time   "
                        << getCycleStartTime(generator) << " us, cycle work     "
                        << getCycleWork(generator) << " us, cycle sleep     "
                        << getCycleSleep(generator) << " us, cycle slept     "
                        << getCycleSleept(generator) << " us, cycle overwork   "
                        << getCycleOverwork(generator) << " us, accum overwork       "
                        << getAccumOverwork(generator) << " us" << endl;

            if (d(generator) >= 0.75) {
                outputFile << "23:03:02.625341 pri=6:MAIN_THREAD:00000000 [src/server.c:3412] sent (len 77) to Stream Client TCP 192.168.134.106:51408 on fd 18:" << endl
                            << "RtpLoadRtpMsg" << endl
                            << "TID:-2030665101" << endl
                            << "Msg:RTPMSG_OPEN_SESSION" << endl
                            << "TimeUs:0x0fca2d8a^M" << endl;
            }
        }
        outputFile.flush();
        outputFile.close();
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }

    return 0;
}