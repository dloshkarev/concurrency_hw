package e2e

import (
	"concurrency_hw/internal/database/network"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const longValue = "qwertyuiopasdrftghjnksdcdsiyvcbasoipdmqwipwgdutyeqwfdiyuqwn;djk1ndlhjqwbviduyqwhpdiqwndljhqwbidytqgodiuwqn;dkjwqbdiygqvwuoydnqwjkdbwqudbqwiudmqw'dnqiuwdbquwnd;owq"

func TestTCPServer(t *testing.T) {
	address := "localhost:3223"

	cmd := exec.Command("../server")
	require.NoError(t, cmd.Start())

	time.Sleep(5 * time.Second)
	cli := getClient(address)

	response, err := cli.Execute("GET q")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(response))

	response, err = cli.Execute("SET q 1")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	response, err = cli.Execute("SET w 2")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	response, err = cli.Execute("GET q")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, "1"), string(response))

	response, err = cli.Execute("GET w")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, "2"), string(response))

	response, err = cli.Execute("DEL w")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, network.SuccessCommand, string(response))

	response, err = cli.Execute("GET q")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, "1"), string(response))

	response, err = cli.Execute("GET w")
	fmt.Println(string(response))
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprintf(network.GetResult, ""), string(response))

	// Проверка WAL
	for range 1000 {
		_, err := cli.Execute(fmt.Sprintf("SET wal %v", longValue))
		require.NoError(t, err)
	}

	require.NoError(t, cli.Disconnect())
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	time.Sleep(time.Second)

}

func getClient(address string) *network.TCPClient {
	client, err := network.NewTCPClient(address)
	if err != nil {
		log.Fatal(err)
	}

	return client
}
