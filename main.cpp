#include "xmldriver.h"

int main(int argc, char* argv[])
{
    try {
        StreamingXml::XmlDriver driver;
        bool showUsage = driver.Initialize(argc, argv);
        if (showUsage) {
            std::cout << "Example: cat tutorial/orders.csv | proj category sum[sales]" << std::endl;
            std::cout << "For more information, open README.MD." << std::endl;
            return 0;
        }
        return driver.Run();
    }
    catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
}
