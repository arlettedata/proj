#include "xmldriver.h"

int main(int argc, char* argv[])
{
    try {
        StreamingXml::XmlDriver driver;
        bool showUsage = driver.Initialize(argc, argv);
        if (showUsage) {
            std::cout << "Example: cat file.json | proj id sum[cost]" << std::endl;
            std::cout << "For complete documentation, open README.MD." << std::endl;
            return 0;
        }
        return driver.Run();
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}
